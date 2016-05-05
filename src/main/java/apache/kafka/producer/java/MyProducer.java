/**
 * Copyright 2016 Ankit Sarraf
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package apache.kafka.producer.java;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;

/**
 * Project Name: KafkaProducer (Java)
 * Dated: April 04, 2016
 */

class MyProducer {
    // Variables
    private String topic;
    private int events;
    private long intraEventDuration;

    // Kafka Producer
    private Producer<String, String> kafkaProducer;

    MyProducer(String kafkaBroker, String topic, int events, long intraEventDuration) {
        this.topic = topic;
        this.events = events;
        this.intraEventDuration = intraEventDuration;

        // setting up properties to be used to communicate to kafka
        Properties props = new Properties();
        props.put("metadata.broker.list", kafkaBroker);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);

        kafkaProducer = new Producer<>(config);
    }

    void processAndPush() throws InterruptedException {
        String myData;
        Random random = new Random();

        KeyedMessage<String, String> message;

        for(int i = 0 ; i <= this.events ; i++) {
            myData = "<" + random.nextFloat() + ">";
            message = new KeyedMessage<>(this.topic, myData);
            System.out.println("Data Produced => " + message);
            this.kafkaProducer.send(message);

            Thread.sleep(this.intraEventDuration);
        }

        kafkaProducer.close();
    }
}
