package com.example.demo.filter;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
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
        //SQLConsumer();
        TagConsumer();
    }

    /**
     * 基于 Tag 的消息过滤
     *
     * @throws Exception
     */
    public static void TagConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        consumer.setNamesrvAddr("localhost:9876;localhost:9877");
        consumer.subscribe("base", "tag1");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.println(list);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
    }

    /**
     * 基于 SQL语法 的消息过滤
     *
     * @throws Exception
     */
    public static void SQLConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        consumer.setNamesrvAddr("localhost:9876;localhost:9877");
        consumer.subscribe("base", MessageSelector.bySql("i>5"));
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.println(list);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
    }
}
