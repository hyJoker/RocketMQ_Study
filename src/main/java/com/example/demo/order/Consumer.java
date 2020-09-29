package com.example.demo.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @ClassName Consumer
 * @Description
 * @Author Rex
 * @Date 2020-09-24 12:55
 * @Version 1.0
 **/
public class Consumer {

    public static void main(String[] args) throws Exception {
        consumer();
    }

    /**
     * 默认负载均衡
     *
     * @throws Exception
     */
    public static void consumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        consumer.setNamesrvAddr("localhost:9876;localhost:9877");
        consumer.subscribe("base", "*");
        // 对于同一个 MessageQueue ,使用一个线程消费
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                System.out.println(Thread.currentThread().getName() + "---" + list);
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
    }
}
