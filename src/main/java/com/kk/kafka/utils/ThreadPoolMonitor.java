package com.kk.kafka.utils;


import java.util.concurrent.ThreadPoolExecutor;

/**
 * 线程池工具类，监视ThreadPoolExecutor执行情况
 */
public class ThreadPoolMonitor implements Runnable{
    private ThreadPoolExecutor executor;
    private volatile boolean isStopMonitor = false;
    private String name = "";
    public ThreadPoolMonitor(ThreadPoolExecutor executor, String name){
        this.executor = executor;
        this.name = name;
    }

    public void run(){
        while(!isStopMonitor){
            System.out.println(name +
                    String.format("[monitor] [%d/%d] Active: %d, Completed: %d, queueSize: %d, Task: %d, isShutdown: %s, isTerminated: %s",
                            this.executor.getPoolSize(),
                            this.executor.getCorePoolSize(),
                            this.executor.getActiveCount(),
                            this.executor.getCompletedTaskCount(),
                            this.executor.getQueue().size(),
                            this.executor.getTaskCount(),
                            this.executor.isShutdown(),
                            this.executor.isTerminated()));
            try {
                Thread.sleep(20000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.out.println("InterruptedException" + e);
            }
        }
    }

    public void setStopMonitor(boolean isStopMonitor) {
        this.isStopMonitor = isStopMonitor;
    }
}