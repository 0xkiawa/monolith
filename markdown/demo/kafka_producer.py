# Copyright 2022 ByteDance and/or its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from ml_dataset import get_preprocessed_dataset, serialize_one
from tqdm import tqdm
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    try:
        # Load preprocessed dataset
        ds = get_preprocessed_dataset()
        
        # Initialize Kafka producer
        producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'])
        logger.info("Kafka producer initialized.")
        
        # Iterate over dataset and send messages to Kafka
        for count, val in tqdm(enumerate(ds), total=len(ds)):
            try:
                producer.send(
                    "movie-train", 
                    key=str(count).encode('utf-8'), 
                    value=serialize_one(val), 
                    headers=[]
                ).add_callback(lambda metadata: logger.info(f"Message sent to {metadata.topic} partition {metadata.partition}"))
                 .add_errback(lambda error: logger.error(f"Error sending message: {error}"))
            except KafkaError as e:
                logger.error(f"Kafka error occurred: {e}")

        # Ensure all messages are sent before exiting
        producer.flush()
        logger.info("All messages successfully sent and flushed.")

    except Exception as e:
        logger.exception("An unexpected error occurred:")

    finally:
        # Cleanup Kafka producer
        if 'producer' in locals():
            producer.close()
            logger.info("Kafka producer closed.")
