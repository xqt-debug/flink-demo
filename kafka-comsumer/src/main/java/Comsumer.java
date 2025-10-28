import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Comsumer {
    static final String HOST = "192.168.118.129:9091,192.168.118.129:9092,192.168.118.129:9093";
    public static void main(String[] args) {
        String topic = "unprocessedData";
        Properties props = getProperties();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(topic));
            System.out.println("已订阅主题: " + topic);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if(records.count() > 0) {
                    for (ConsumerRecord<String, String> record : records) {
                        String[] msg = record.value().split(",");

                        SqlInserter.insert(
                                msg[0],msg[1],
                                Integer.parseInt(msg[2]),
                                Integer.parseInt(msg[3])
                        );
                        System.out.printf("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s\n"
                                , record.topic(), record.partition(),
                                record.offset(), record.key(), record.value());

                    }
                    consumer.commitSync();
                }
            }
        }
        catch (Exception e) {
            System.err.println("消费时发生错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
    private static Properties getProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", HOST);
        props.put("group.id", "xqt");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        return props;
    }
}
