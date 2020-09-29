package com.example.demo.filter;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * @ClassName Producer
 * @Description
 * @Author Rex
 * @Date 2020-09-24 12:55
 * @Version 1.0
 **/
public class Producer {

    public static void main(String[] args) throws Exception {
        SyncProducer();
        //AsyncProducer();
        //OneWayProducer();
    }

    /**
     * 发送同步消息
     *
     * @throws Exception
     */
    public static void SyncProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        producer.setNamesrvAddr("localhost:9876;localhost:9877");
        producer.start();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("base", "tag1", ("Hello World " + i).getBytes());
            message.putUserProperty("i", i + "");
            SendResult result = producer.send(message);
            System.out.println(result.getSendStatus() + "----" + result.getMsgId() + "----" + result.getMessageQueue().getQueueId());
        }
        producer.shutdown();
    }

    /**
     * 发送异步消息
     *
     * @throws Exception
     */
    public static void AsyncProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        producer.setNamesrvAddr("localhost:9876;localhost:9877");
        producer.start();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("base", "tag2", ("Hello World " + i).getBytes());
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println("发送成功:" + sendResult);
                }

                @Override
                public void onException(Throwable throwable) {
                    System.out.println("发送失败:" + throwable);
                }
            });
        }
        Thread.sleep(1000);
        producer.shutdown();
    }

    /**
     * 发送单向消息
     *
     * @throws Exception
     */
    public static void OneWayProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        producer.setNamesrvAddr("localhost:9876;localhost:9877");
        producer.start();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("base", "tag3", ("Hello World " + i).getBytes());
            producer.sendOneway(message);
        }
        producer.shutdown();
    }
}
