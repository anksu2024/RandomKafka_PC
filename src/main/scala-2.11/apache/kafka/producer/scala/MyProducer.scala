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

import java.util.{Properties, Random}

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}

/**
  * Project Name: KafkaProducer (Scala)
  * Dated: April 28, 2016
  */

class MyProducer private (val kafkaBroker: String, val topic: String,
                 val events: Int, val intraEventDuration: Long,
                 var props: Properties, var config: ProducerConfig,
                 var kafkaProducer: Producer[String, String]) {

  def this(kafkaProducer: String, topic: String, events: Int, intraEventDuration: Long) =
    this(kafkaBroker,topic,events,intraEventDuration,null,null,null)

  // Setting up properties to be used to communicate to kafka
  props = new Properties
  props.put("metadata.broker.list", kafkaBroker)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("partitioner.class", "kafka.producer.DefaultPartitioner")
  props.put("request.required.acks", "1")

  config = new ProducerConfig(props)
  kafkaProducer = new Producer(config)

  def processAndPush(): Unit = {
    var myData: String = ""
    val random = new Random

    var message: KeyedMessage[String, String] = null

    for (i <- 0 to this.events) {
      myData = "<" + random.nextFloat + ">"
      message = new KeyedMessage[String, String](this.topic, myData)
      println("Data Produced -> " + message)
      this.kafkaProducer.send(message)

      Thread.sleep(this.intraEventDuration)
    }

    kafkaProducer.close
  }
}
