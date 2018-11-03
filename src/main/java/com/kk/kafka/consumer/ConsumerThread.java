package com.kk.kafka.consumer;


import com.kk.kafka.consts.KafkaConsts;
import com.kk.kafka.utils.PropertiesParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

public class ConsumerThread implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private String topic ;
    private String groupId ;
    private boolean stop;
    private BlockingQueue<String> queue;
    private static PropertiesParser prop = new PropertiesParser(KafkaConsts.CONF_FILE);
    private static String brokerList = prop.getStringProperty(KafkaConsts.BROKER_LIST);
    private static String autoCommit = prop.getStringProperty(KafkaConsts.AUTO_COMMIT);
    private static String intervalMs = prop.getStringProperty(KafkaConsts.INTERVAL_MS);
    private static String timeoutMs = prop.getStringProperty(KafkaConsts.TIMEOUT_MS);
    private static String reset = prop.getStringProperty(KafkaConsts.OFFSET_RESET);


    public ConsumerThread(BlockingQueue<String> queue, String topic, String groupId, boolean stop) {
        this.queue = queue;
        this.topic = topic;
        this.groupId = groupId;
        this.stop = stop;
        Properties prop = createConsumerConfig();
        this.consumer = new KafkaConsumer<String, String>(prop);
        this.consumer.subscribe(Arrays.asList(this.topic));
    }

    private Properties createConsumerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", autoCommit);//true时，Consumer会在消费消息后将offset同步到zookeeper，这样当Consumer失败后，新的consumer就能从zookeeper获取最新的offset
        props.put("auto.commit.interval.ms", intervalMs);//自动提交的时间间隔
        props.put("session.timeout.ms", timeoutMs);//超时时间，超过时间会认为是无效的消费者
        props.put("auto.offset.reset", reset);//当zookeeper中没有初始的offset时，或者超出offset上限时的处理方式 ;默认值为latest，表示从topic的的尾开始处理，earliest表示从topic的头开始处理
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    @Override
    public String toString() {
        return "ConsumerThread{" +
                "topic='" + topic + '\'' +
                ", stop=" + stop +
                '}';
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(60000);
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        queue.put(record.value());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Receive message: " + ", Offset: " + record.offset() + ", By ThreadID: "
                            + Thread.currentThread().getName());
                }
            } catch (KafkaException e) {
                System.out.println("The Exception is :" + e);
            }
        }
    }
    public void setStop(boolean stop) {
        this.stop = stop;
    }

    public boolean isStop() {
        return stop;
    }
}
