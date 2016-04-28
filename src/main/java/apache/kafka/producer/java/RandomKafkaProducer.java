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
