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

  group_id = f"client-consumer-{int(time.time())}"
  consumer =  KafkaConsumer(
    'messages',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id=group_id,
  )

  schemaArray = []
  try:
    message = next(consumer)
    data = json.loads(message.value)
    print(f"Message: {data}")
  
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
  
  # create destination table
  create_destination_table = f"CREATE TABLE IF NOT EXISTS {messages} ("\
                              f"{columns}) "\
                              f"ENGINE = MergeTree "\
                              f"ORDER BY (timestamp) "\
                              f"PRIMARY KEY ({col_name})"

  # create kafka table engine table
  create_engine_table = f"CREATE TABLE IF NOT EXISTS kafka_events_raw "\
                        f"({columns}) "\
                        f"ENGINE = Kafka "\
                        f"SETTINGS "\
                        f"kafka_broker_list = 'localhost:9092', "\
                        f"kafka_topic_list = 'messages', "\
                        f"kafka_group_name = 'clickhouse-consumer', "\
                        f"kafka_format = 'JSONEachRow', "\
                        f"kafka_num_consumers = 1;"
  
  # create materialized view
  create_materialized_view = f"CREATE MATERIALIZED VIEW IF NOT EXISTS mv_{messages} "\
                              f"TO messages "\
                              f"AS "\
                              f"SELECT {column_names} "\
                              f"FROM kafka_events_raw;"
  
  ch_client.command(create_destination_table)
  ch_client.command(create_engine_table)
  ch_client.command(create_materialized_view)

if __name__ == '__main__':
  main()
