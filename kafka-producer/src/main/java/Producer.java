import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class Producer {
    static final String HOST =
            "192.168.118.129:9091,192.168.118.129:9092,192.168.118.129:9093";

    public static void main(String[] args) {
        Properties props = getProperties();
        org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(props);
        Random random = new Random();
        try {
            while (true) {
                for (int i = 1; i <= 10; i++) {
                    for(int j = 1; j <= 10; j++) {
                        int temperature = random.nextInt(51) + 30;//服务器温度30~80
                        int voltage = random.nextInt(45) + 198;//服务器电压198~244
                        String msg = "机房" + i + ","
                                + "机柜" + j + ","
                                + temperature + ","
                                + voltage;
                        producer.send(new ProducerRecord<>("unprocessedData", msg));
                        System.out.println("Send:" + msg);
                    }
                }
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            producer.close();
        }

    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}