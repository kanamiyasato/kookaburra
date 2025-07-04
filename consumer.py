import json
import os
from kafka import KafkaConsumer
import time
import clickhouse_connect

def main():
  ch_client = clickhouse_connect.get_client(
    host=os.getenv('CLICKHOUSE_HOST'),
    port=os.getenv('CLICKHOUSE_PORT'),
    username=os.getenv('CLICKHOUSE_USER'),
    password=os.getenv('CLICKHOUSE_PASSWORD')
  )

  def get_kafka_topics(bootstrap_servers):
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
    topics = consumer.topics()
    consumer.close()
    return sorted(topics)
  
  topics = get_kafka_topics("localhost:9092")
  print("Choose a topic to start reading from:")
  for topic in topics:
    print(topic)
  selected_topic = input()

  print(f"Reading from '{selected_topic}' topic")


  group_id = f"client-consumer-{int(time.time())}"
  consumer =  KafkaConsumer(
    selected_topic,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id=group_id,
  )

  schemaArray = []
  try:
    message = next(consumer)
    data = json.loads(message.value)
    print(f"Message found: {data}")
  
    json_string = json.dumps(data)
    ch_client.command("SET schema_inference_make_columns_nullable = 0;")
    ch_client.command("SET input_format_null_as_default = 0;")
    res = ch_client.query(f"DESC format(JSONEachRow, '{json_string}');")

    for row in res.result_rows:
      schema = {
        'name': row[0],
        'type': row[1],
      }
      schemaArray.append(schema)
    print(schemaArray)
  except StopIteration:
    print("No messages found in the topic.")
  finally:
    consumer.close()
  
  columns = ", ".join([f'{col["name"]} {col["type"]}' for col in schemaArray])
  column_names = ", ".join([col['name'] for col in schemaArray])
  
  # create database
  create_database = "CREATE DATABASE IF NOT EXISTS kafka_engine"

  # create destination table
  create_destination_table = f"CREATE TABLE IF NOT EXISTS kafka_engine.{selected_topic} ("\
                              f"{columns}) "\
                              f"ENGINE = MergeTree "\
                              f"ORDER BY "

  # create kafka table engine table
  create_engine_table = f"CREATE TABLE IF NOT EXISTS kafka_engine.kafka_events_raw "\
                        f"({columns}) "\
                        f"ENGINE = Kafka "\
                        f"SETTINGS "\
                        f"kafka_broker_list = 'localhost:9092', "\
                        f"kafka_topic_list = '{selected_topic}', "\
                        f"kafka_group_name = 'clickhouse-consumer', "\
                        f"kafka_format = 'JSONEachRow', "\
                        f"kafka_num_consumers = 1, "\
                        f"kafka_auto_offset_reset = 'earliest';"
  
  # create materialized view
  create_materialized_view = f"CREATE MATERIALIZED VIEW IF NOT EXISTS kafka_engine.mv_{selected_topic} "\
                              f"TO kafka_engine.{selected_topic} "\
                              f"AS "\
                              f"SELECT {column_names} "\
                              f"FROM kafka_events_raw;"
  
  ch_client.command(create_destination_table)
  ch_client.command(create_engine_table)
  ch_client.command(create_materialized_view)

if __name__ == '__main__':
  main()
