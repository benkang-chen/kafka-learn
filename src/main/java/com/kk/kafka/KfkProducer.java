package com.kk.kafka;

import com.kk.kafka.utils.PropertiesParser;
import com.kk.kafka.consts.KafkaConsts;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.*;

import java.util.List;
import java.util.Properties;

public class KfkProducer {
    private static Producer<String, String> producer;
    private static PropertiesParser prop = new PropertiesParser(KafkaConsts.CONF_FILE);
    private static String brokerList = prop.getStringProperty(KafkaConsts.BROKER_LIST);
    private static String acks = prop.getStringProperty(KafkaConsts.ACKS);
    private static String retries = prop.getStringProperty(KafkaConsts.RETRIES);
    private static String batchSize = prop.getStringProperty(KafkaConsts.BATCH_SIZE);
    private static String lingerMs = prop.getStringProperty(KafkaConsts.LINGER_MS);
    private static String bufferMemory = prop.getStringProperty(KafkaConsts.BUFFER_MEMORY);
    private static String maxRequestSize = prop.getStringProperty(KafkaConsts.MAX_REQUEST_SIZE);
    private static KfkProducer kfkProducer;

    public synchronized static KfkProducer getInstance(){

        try {
            if(kfkProducer == null || kfkProducer.producer == null){
                synchronized (KfkProducer.class) {
                    if(kfkProducer == null || kfkProducer.producer == null){
                        kfkProducer = null;
                        kfkProducer = new KfkProducer();
                    }
                }
            }
            //应对zk挂了的情况
        } catch (ZkTimeoutException e) {
            e.printStackTrace();
            return getInstance();
        }
        return kfkProducer;
    }

    private KfkProducer() {
        Properties prop = createProducerConfig();
        producer = new KafkaProducer<String, String>(prop);
    }

    public static Properties createProducerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        //所有follower都响应了才认为消息提交成功，即"committed"
        props.put("acks", acks);
        //消息发送失败的情况下，重试发送的次数;如果不为0，则存在消息发送是成功的，只是由于网络导致ACK没收到的重试，会出现消息被重复发送的情况;
        props.put("retries", retries);
        //Producer会尝试去把发往同一个Partition的多个Requests进行合并，batch.size指明了一次Batch合并后Requests总大小的上限。
        // 如果这个值设置的太小，可能会导致所有的Request都不进行Batch（16KB）。
        props.put("batch.size", batchSize);
        //Producer默认会把两次发送时间间隔内收集到的所有Requests进行一次聚合然后再发送，以此提高吞吐量，而linger.ms则更进一步，这个参数为每
        // 次发送增加一些delay，以此来聚合更多的Message。
        props.put("linger.ms", lingerMs);
        //在Producer端用来存放尚未发送出去的Message的缓冲区大小。缓冲区满了之后可以选择阻塞发送或抛出异常，由block.on.buffer.full的
        // 配置来决定。（32MB）block.on.buffer.full这个配置是默认值是flase，也就是当bufferpool满时，
        // 不会抛出BufferExhaustException，而是根据max.block.ms进行阻塞，如果超时抛出TimeoutExcpetion。
        props.put("buffer.memory", bufferMemory);
        props.put("max.request.size", maxRequestSize);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }


    public boolean send(String topic,String message){
        //应对kafka挂了的情况
        while(!executeSend(topic, message)){
            System.out.println("send message error!");
        }
        return true;
    }
    public boolean send(String topic, List<String> messages){
        while(!executeSend(topic, messages)){
            System.out.println("send message error!");
        }
        return true;
    }
    public boolean executeSend(String topic, final String message) {
        if (StringUtils.isEmpty(topic) || StringUtils.isEmpty(message)) {
            System.out.println("before send msg get topic or message -> null, " );
            return true;
        }
        while(this.producer==null){
            Properties prop = createProducerConfig();
            producer = new KafkaProducer<String, String>(prop);
        }
//        System.out.println("sending message to topic -> " + topic + ", " +" message: " + message);
        try {
            this.producer.send(new ProducerRecord<String, String>(topic, message), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    }
                    System.out.println("Sent:" + message + ", Partition: " + metadata.partition() + ", Offset: "
                            + metadata.offset());
                }
            });
        } catch (Exception e) {
            System.out.println("send data exception, err=" + e);
            this.close();
            return false;
        }
//        System.out.println("sending message successful! topic: " + topic + " message: " + message);
        return true;
    }

    public boolean executeSend(String topic, List<String> messages) {
        if (messages==null || messages.size()==0) {
            System.out.println("before send msg get topic or message -> null, projName=" );
            return true;
        }
        while(this.producer==null){
            Properties prop = createProducerConfig();
            producer = new KafkaProducer<String, String>(prop);
        }

        try {
            for(final String message : messages){
                System.out.println("batch producer sending message to topic -> " + topic + ", message -> "+ messages.toString());
                this.producer.send(new ProducerRecord<String, String>(topic, message), new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null) {
                            e.printStackTrace();
                        }
                        System.out.println("Sent:" + message + ", Partition: " + metadata.partition() + ", Offset: "
                                + metadata.offset());
                    }
                });
                System.out.println("batch sending message successful! topic: " + topic + " message: " + message);
            }
            System.out.println("sended messages to topic -> " + topic);
        } catch (Exception e) {
            System.out.println(Thread.currentThread().getName()+":topic is "+topic+".message size is "+messages.size()+".send data exception, err=" + e);
            this.close();
            return false;
        }
        return true;
    }

    public void close() {
        if (producer != null) {
            try {
                producer.close();
                System.out.println("Producter closed!");
            } catch (Exception e) {
                System.out.println("close producter exception, err=" + e);
            } finally {
                producer = null;
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        long begin = System.currentTimeMillis();
        String str = "在提高效率方面做了很大努力。Kafka的一个主要使用场景是处理网站活动日志，吞吐量是非常大的，每个页面都会产生好多次写操作。读方面，假设每个消息只被消费一次，读的量的也是很大的，Kafka也尽量使读的操作更轻量化。";
        System.out.println("message length : "+str.getBytes().length);
        String topic = "kafka_test_job1";
        for (int i = 0; i < 10; i++) {
            KfkProducer.getInstance().send(topic, str);
        }
        Thread.sleep(100000);
        long end = System.currentTimeMillis();
//        System.out.println("begin:"+begin+";end:"+end);
    }
}
