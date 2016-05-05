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
 
package apache.kafka.producer.scala

/**
  * Project Name: KafkaProducer (Scala)
  * Dated: April 28, 2016
  */

object RandomKafkaProducer {
  def main(args: Array[String]) {
    if(args.length != 4) {
      println("Usage: java -cp <kafka-broker:port>" +
        "<topic> <# of events> <duration between each event>")
      return
    }

    val kafkaBroker = args(0)
    val topic = args(1)
    val events = args(2).toInt
    val intraEventDuration = args(3).toLong

    println("Hello from Random Kafka Producer!!")
    val kafka_producer = new MyProducer(kafkaBroker, topic, events, intraEventDuration)

    println("Sending Producer Data!!")

    kafka_producer.processAndPush()

    println("Data Sent to the Broker.. Pull Data from Consumer!!")
  }
}
