import os
import json
import logging
import tensorflow as tf
import numpy as np
import uuid 
import time 
import joblib 
import sys
from datetime import datetime 
from sklearn.preprocessing import StandardScaler
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import ProcessFunction, RuntimeContext, MapFunction
from pyflink.common.configuration import Configuration
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from kafka import KafkaConsumer, KafkaProducer
# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('traffic_prediction.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('traffic_prediction')

def log_data(data):
    print(f"Data type: {type(data)}, Value: {data}")
    return data
class PredictionFileWriter(MapFunction):
    def __init__(self, file_path):
        self.file_path = file_path
        
    def map(self, value):
        print ("starting write to file")
        record = {
            "id": str(uuid.uuid4()),
            "prediction": value
        }
        with open(self.file_path, 'a') as f:
            json.dump(record, f)
            f.write('\n')
        return value
class KafkaFileHandler(FileSystemEventHandler):
    def __init__(self, file_path, bootstrap_servers, topic):
        print("start init for file handler")
        self.file_path = file_path
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version = (0,10),
            acks = 'all',
            retries = 5
        )
        self.topic = topic
        self.last_position = 0            # Track where we left off reading
        self.sent_ids = set()             # Track sent messages

    def on_modified(self, event):
        if event.src_path == self.file_path:
            print("something")
            with open(self.file_path, 'r') as f:
                f.seek(self.last_position)  # Go to last read position
                while True:
                    line = f.readline()     # Read line by line
                    if not line:            # EOF reached
                        break
                        
                    try:
                        data = json.loads(line.strip())
                        if data["id"] not in self.sent_ids:  # Check if new
                            future = self.producer.send(self.topic, value=data)
                            result = future.get(timeout = 5)
                            # print(f"Message sent:{data}")
                            self.sent_ids.add(data["id"])
                    except Exception as e :
                        print(f"error {e}")
                    
                self.last_position = f.tell()  # Update position
    def run(self):
    # Get directory containing the file
        directory = os.path.dirname(os.path.abspath(self.file_path))
        print(f"Monitoring directory:{directory}")
        print(f"Watching file:{self.file_path}")
        
        observer = Observer()
        # Monitor only our specific file
        
        
        observer.schedule(self, directory, recursive=False)
        observer.start()
        print("observer started")
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
            self.producer.close()
        observer.join()
    # Get directory containing the file
class LSTMModel:
    def __init__(self):
        logger.info("Initializing LSTM model and normalizer")
        print("Initializing LSTM model and normalizer")
        try:
            model_path = os.path.expanduser("models/traffic_lstm_model")
            scaler_path = os.path.expanduser("models/traffic_scaler.pkl")
            print(f"Model_path:{model_path}")
            print(f"Scalar_path:{scaler_path}")
            if not os.path.exists(model_path):
                raise FileNotFoundError(f"Model directory not found at: {model_path}")
            if not os.path.exists(scaler_path):
                raise FileNotFoundError(f"Scaler file not found at: {scaler_path}")
            print("Loadig Model")
            self.model = tf.keras.models.load_model(
                model_path,
                compile=False,
                options=tf.saved_model.LoadOptions(experimental_io_device='/job:localhost')
            )
            
            expected_shape = (None, 10, 1)
            actual_shape = self.model.input_shape
            logger.info(f"Model input shape: {actual_shape}")
            
            if actual_shape[1:] != expected_shape[1:]:
                raise ValueError(f"Model expects input shape {expected_shape} but got {actual_shape}")
            print("loading normalizer")
            self.normalizer = joblib.load(scaler_path)
            logger.info("Model and normalizer loaded successfully")
            print("Model and normalizer loaded successfully")
            
        except Exception as e:
            logger.error(f"Error loading model or normalizer: {e}")
            raise

    def normalize_data(self, data):
        try:
            data_array = np.array(data).reshape(-1, 1)
            return self.normalizer.transform(data_array).flatten()
        except Exception as e:
            logger.error(f"Normalization error: {e}")
            raise

    def denormalize_prediction(self, prediction):
        try:
            return float(self.normalizer.inverse_transform([[prediction]])[0][0])
        except Exception as e:
            logger.error(f"Denormalization error: {e}")
            raise

    def predict(self, data):
        try:
            logger.info("Starting prediction")
            normalized_data = self.normalize_data(data)
            normalized_data = normalized_data.reshape(1, len(data), 1)
            logger.info(f"Input shape for prediction: {normalized_data.shape}")
            
            normalized_prediction = self.model.predict(
                normalized_data,
                verbose=0
            )
            
            return self.denormalize_prediction(normalized_prediction[0][0])
        except Exception as e:
            logger.error(f"Prediction error: {e}")
            logger.error(f"Data shape: {np.array(data).shape}")
            raise

class TrafficPredictor(ProcessFunction):
    def __init__(self):
        super().__init__()
        self.model = None
        self.sequence_length = 10
        self.sensor_histories = {}
        print("Starting TrafficPredictor init")
        logger.info("Starting TrafficPredictor init")

    def open(self, runtime_context: RuntimeContext):
        print("Entering open method")
        logger.info("Entering open method")
        self.model = LSTMModel()
        logger.info("Process function initialized")
        print("Process function initialized")

    def process_element(self, value, ctx):
        # print(f"ENTERING PROCESS ELEMENT with value: {value}")
        logger.info(f"ENTERING PROCESS ELEMENT with value: {value}")
        start_time = time.time()
        
        try:
            if not value:
                print("Received empty value")
                logger.warning("Received empty value")
                return
                
            # print(f"Value type: {type(value)}")
            # print(f"Value content: {value}")
            
            data = json.loads(value)
            # print(f"Parsed JSON: {data}")
            
            predictions = {}
            
            for sensor_id, speed in data.items():
                if not isinstance(speed, (int, float)):
                    logger.warning(f"Invalid speed value for sensor {sensor_id}: {speed}")
                    continue
                if sensor_id not in self.sensor_histories :
                    self.sensor_histories [sensor_id] = []

                    
                
                self.sensor_histories [sensor_id].append(float(speed))
                
                
                if len(self.sensor_histories [sensor_id]) == self.sequence_length:
                    # print(f"Predicting for sensor {sensor_id}")
                    # print(f"History length: {len(self.sensor_histories [sensor_id])}")
                    # print(f"History data: {self.sensor_histories [sensor_id]}")
                    prediction = self.model.predict(self.sensor_histories [sensor_id])
                    predictions[sensor_id] = float(prediction)
                    self.sensor_histories[sensor_id] = self.sensor_histories[sensor_id][1:]
                    
            
            if predictions:
                result = json.dumps(predictions)
                latency_processing = time.time() - start_time
                print("succesful result")
                # print(f"Returning predictions: {result}")
                print(f"processing latency:{latency_processing}")
                logger.info(f"Returning predictions: {result}")
                yield result
            
            return None
            
        except json.JSONDecodeError as je:
            print(f"JSON Decode Error: {je}")
            logger.error(f"JSON Decode Error: {je}")
            return
        except Exception as e:
            print(f"Error in process_element: {e}")
            logger.error(f"Error in process_element: {e}")
            return

# def verify_kafka_setup():
#     from kafka.admin import KafkaAdminClient
#     from kafka.errors import KafkaError
    
#     try:
#         admin_client = KafkaAdminClient(
#             bootstrap_servers=['localhost:9092'],
#             client_id='admin-client-verify'
#         )
        
#         topics = admin_client.list_topics()
#         print(f"Available Kafka topics: {topics}")
        
#         if 'csv_topic' not in topics:
#             print("WARNING: csv_topic not found in Kafka!")
#             logger.warning("csv_topic not found in Kafka!")
        
#         consumer = KafkaConsumer(
#             'csv_topic',
#             bootstrap_servers=['localhost:9092'],
#             auto_offset_reset='earliest',
#             consumer_timeout_ms=5000
#         )
        
#         print("Checking for messages in csv_topic...")
#         logger.info("Checking for messages in csv_topic...")
#         message_count = 0
#         for msg in consumer:
#             message_count += 1
#             print(f"Found message: {msg.value[:100]}...")
#             if message_count >= 3:
#                 break
        
#         if message_count == 0:
#             print("WARNING: No messages found in csv_topic!")
#             logger.warning("No messages found in csv_topic!")
        
#         consumer.close()
#         admin_client.close()
        
#     except KafkaError as e:
#         print(f"Kafka verification failed: {e}")
#         logger.error(f"Kafka verification failed: {e}")
#         raise

def main():
    try:
        kafka_jar_path = os.path.abspath("/home/avanish/traffic_predictor/lib/flink-sql-connector-kafka-1.17.2.jar")
        if not os.path.exists(kafka_jar_path):
            raise FileNotFoundError(f"Kafka connector JAR not found at: {kafka_jar_path}")
        
        print("Setting up Flink environment...")
        logger.info("Setting up Flink environment...")
        
        config = Configuration()
        config.set_string("pipeline.jars", f"file://{kafka_jar_path}")
        config.set_string("parallelism.default", "1")
        
        env = StreamExecutionEnvironment.get_execution_environment(config)
        env.add_jars(f"file://{kafka_jar_path}")
        
        properties = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'flink_consumer_group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': 'false',
            'debug': 'all',
            'client.id': 'flink-kafka-consumer-debug'
        }

        print(f"Setting up Kafka consumer with properties: {properties}")
        logger.info(f"Setting up Kafka consumer with properties: {properties}")

        kafka_consumer = FlinkKafkaConsumer(
            'csv_topic',
            SimpleStringSchema(),
            properties
        )
        
        kafka_consumer.set_start_from_earliest()
        
        print("Adding source to environment...")
        ds = env.add_source(kafka_consumer)
        # Debug the incoming data
        # ds = ds.map(lambda x: print(f"Pre-process value: {x}") or x)
        
        print("Setting up prediction stream...")
        predictor = TrafficPredictor()
        prediction_stream = ds.process(predictor)
        # prediction_stream.print()
        
        # Debug the processed data
        # prediction_stream = prediction_stream.map(lambda x: print(f"Post-process value: {x}") or x)
        
        kafka_producer_props = {
            'bootstrap.servers': 'localhost:9092',
            'retries': '5',
            'batch.size': '16384',
            'linger.ms': '10',
            'acks': 'all',
        }
        file_writer = PredictionFileWriter("predictions.jsonl")
        # kafka_sink = FlinkKafkaProducer(
        #     topic="predictions_topic",
        #     serialization_schema=SimpleStringSchema(),
        #     producer_config=kafka_producer_props
        # )
        # prediction_stream.add_sink(kafka_sink)
        prediction_stream = prediction_stream.map(file_writer)
        event_handler = KafkaFileHandler(
            os.path.abspath("predictions.jsonl"),
            bootstrap_servers=['localhost:9092'],  # Replace with target Kafka broker
            topic='predictions_topic'
        )
        import threading
        kafka_thread = threading.Thread(target=event_handler.run)
        kafka_thread.daemon = False
        kafka_thread.start()
        
        print("Starting Flink execution...")
        logger.info("Starting Flink execution...")
        env.execute("Traffic Speed Prediction Job")
        
    except Exception as e:
        print(f"Main function error: {e}")
        logger.error(f"Error in execution: {e}", exc_info=True)
        raise
    except KeyboardInterrupt:
        kafka_thread.join()
        sys.exit(0)

print("Starting Traffic Prediction Application...")
logger.info("Starting Traffic Prediction Application...")
# verify_kafka_setup()
main()
print("Application finished")
logger.info("Application finished")