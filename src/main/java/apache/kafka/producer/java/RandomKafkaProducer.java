package apache.kafka.producer.java;

/**
 * Project Name: RandomKafkaProducer (Java)
 * Dated: April 04, 2016
 */

public class RandomKafkaProducer {
    public static void main(String... args) throws InterruptedException {
        if (args.length != 4) {
            System.out
                    .println("Usage: java -cp <kafka-broker:port> <topic> <# of events> <duration between each event>");
            return;
        }

        String kafkaBroker = args[0];
        String topic = args[1];
        int events = Integer.parseInt(args[2]);
        long intraEventDuration = Long.parseLong(args[3]);

        System.out.println("Hello from Random Kafka Producer!!");
        MyProducer kakfa_producer = new MyProducer(kafkaBroker, topic, events, intraEventDuration);

        System.out.println("Sending Producer Data!!");

        kakfa_producer.processAndPush();

        System.out.println("Data send to the broker.. Pull Data from Consumer!!");
    }
}