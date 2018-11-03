package com.kk.kafka.consts;


import com.kk.kafka.consumer.ConsumerGroup;

import java.util.HashMap;
import java.util.Map;

public class KafkaConsts {
    public static final String CONF_FILE = "../etc/kafka.properties";
    public static final String BROKER_LIST = "crawl.kfk.metadata.broker.list";
    public static final String ACKS = "crawl.kfk.producer.acks";
    public static final String RETRIES = "crawl.kfk.producer.retries";
    public static final String BATCH_SIZE = "crawl.kfk.producer.batch.size";
    public static final String LINGER_MS = "crawl.kfk.producer.linger.ms";
    public static final String BUFFER_MEMORY = "crawl.kfk.producer.buffer.memory";
    public static final String MAX_REQUEST_SIZE = "crawl.kfk.producer.max.request.size";
    public static final String AUTO_COMMIT = "crawl.kfk.consumer.enable.auto.commit";
    public static final String INTERVAL_MS = "crawl.kfk.consumer.auto.commit.interval.ms";
    public static final String TIMEOUT_MS = "crawl.kfk.consumer.session.timeout.ms";
    public static final String OFFSET_RESET = "crawl.kfk.consumer.auto.offset.reset";
    public static final String CORE_POOL_SIZE = "crawl.kfk.consumer.thread.core.pool.size";
    public static final String MAXIMNM_POOL_SIZE = "crawl.kfk.consumer.thread.maximum.pool.size";
    public static final Map<String, ConsumerGroup> consumerGroupMap = new HashMap();
}
