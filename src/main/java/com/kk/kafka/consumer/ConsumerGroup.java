package com.kk.kafka.consumer;


import com.kk.kafka.consts.KafkaConsts;
import com.kk.kafka.utils.PropertiesParser;
import com.kk.kafka.utils.SimpleThreadPoolExecutor;
import com.kk.kafka.utils.ThreadPoolMonitor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConsumerGroup {

    private static PropertiesParser prop = new PropertiesParser(KafkaConsts.CONF_FILE);
    private static int corePoolSize = prop.getIntProperty(KafkaConsts.CORE_POOL_SIZE);
    private static int maximumPoolSize = prop.getIntProperty(KafkaConsts.MAXIMNM_POOL_SIZE);
    private final int numberOfConsumers;
    private String topic;
    private String groupId;
    private BlockingQueue<String> queue;
    private List<ConsumerThread> consumers;
    private ThreadPoolExecutor consumerThreadPool;
    private ThreadPoolMonitor threadPoolMonitor;

    public ConsumerGroup(int numberOfConsumers, String topic, String groupId, BlockingQueue<String> queue) {
        this.numberOfConsumers = numberOfConsumers;
        this.topic = topic;
        this.groupId = groupId;
        this.queue = queue;
        consumers = new ArrayList<ConsumerThread>();
        this.consumerThreadPool = new SimpleThreadPoolExecutor(corePoolSize,
                maximumPoolSize,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(2000),
                new ThreadPoolExecutor.DiscardPolicy(),
                "consumer" + "(" + this.topic + ")" + "ThreadPool");
        this.threadPoolMonitor = new ThreadPoolMonitor(consumerThreadPool, "consumer" + "(" + this.topic + ")" + "ThreadPool");
        for (int i = 0; i < this.numberOfConsumers; i++) {
            ConsumerThread ncThread = new ConsumerThread(this.queue, this.topic, this.groupId, false);
            consumers.add(ncThread);
        }
    }

    public void execute() {
        for (ConsumerThread ncThread : consumers) {
            Thread t = new Thread(ncThread);
            consumerThreadPool.execute(t);
        }
        new Thread(threadPoolMonitor).start();
    }

    public void start() {
        for (ConsumerThread ncThread : consumers) {
            if (ncThread.isStop() == true) {
                ncThread.setStop(false);
                Thread t = new Thread(ncThread);
                consumerThreadPool.execute(t);
            }
        }
    }

    public void stop(){
        for (ConsumerThread ncThread : consumers) {
            System.out.println(ncThread.toString());
            ncThread.setStop(true);
        }
    }

    public ThreadPoolExecutor getConsumerThreadPool(){
        return this.consumerThreadPool;
    }

    public ThreadPoolMonitor getThreadPoolMonitor() {
        return this.threadPoolMonitor;
    }
}
