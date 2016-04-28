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