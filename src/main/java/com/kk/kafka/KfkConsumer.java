package com.kk.kafka;

import com.kk.kafka.consts.KafkaConsts;
import com.kk.kafka.consumer.ConsumerGroup;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class KfkConsumer {

    public static void startReadThread(final BlockingQueue<String> queue, String topic, int threadNumPerTopic, String groupId){
        if (!KafkaConsts.consumerGroupMap.containsKey(topic)) {
            ConsumerGroup consumerGroup = new ConsumerGroup(threadNumPerTopic, topic, groupId, queue);
            KafkaConsts.consumerGroupMap.put(topic, consumerGroup);
            consumerGroup.execute();
        } else {
            ConsumerGroup consumerGroup = KafkaConsts.consumerGroupMap.get(topic);
            consumerGroup.start();
        }
    }

    public static void stopKfkConsumerThreads(){
        for (ConsumerGroup consumerGroup: KafkaConsts.consumerGroupMap.values()) {
            consumerGroup.stop();
            consumerGroup.getConsumerThreadPool().shutdown();
            consumerGroup.getThreadPoolMonitor().setStopMonitor(true);
        }
    }

    public static void stopKfkConsumerThreads(String topic){
        if (KafkaConsts.consumerGroupMap.containsKey(topic)) {
            ConsumerGroup consumerGroup = KafkaConsts.consumerGroupMap.get(topic);
            consumerGroup.stop();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>();
        String topic = "ecTopicSq";
        String groupId = "job1";
//        LinkedBlockingQueue<String> queue1 = new LinkedBlockingQueue<String>();
//        String topic1 = "kafka_test_job1";
//        String groupId1 = "job1";
        startReadThread(queue, topic, 3, groupId);
//        startReadThread(queue1, topic1, 3, groupId1);
        while (true){
            System.out.println("queue: " + queue.take());
//            System.out.println("queue1: " + queue1.size());
            Thread.sleep(1000);
        }
//        System.out.println("queue: " + queue.size());
//        System.out.println("queue1: " + queue1.size());
//        Thread.sleep(6000);
//        stopKfkConsumerThreads("kafka_test_job1");
    }
}
