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

/**
 * Project Name: KafkaProducer (Java)
 * Dated: April 04, 2016
 */

public class RandomKafkaProducer {
    public static void main(String... args) throws InterruptedException {
        if (args.length != 4) {
            System.out
                    .println("Usage: java -cp <kafka-broker:port> <topic> " +
                            "<# of events> <duration between each event>");
            return;
        }

        String kafkaBroker = args[0];
        String topic = args[1];
        int events = Integer.parseInt(args[2]);
        long intraEventDuration = Long.parseLong(args[3]);

        System.out.println("Hello from Random Kafka Producer!!");
        MyProducer kafka_producer = new MyProducer(kafkaBroker, topic, events, intraEventDuration);

        System.out.println("Sending Producer Data!!");

        kafka_producer.processAndPush();

        System.out.println("Data sent to the broker.. Pull Data from Consumer!!");
    }
}
