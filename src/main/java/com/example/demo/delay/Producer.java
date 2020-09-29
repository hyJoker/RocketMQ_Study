package com.example.demo.delay;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
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
        DelayProducer();
    }

    /**
     * 发送同步消息
     *
     * @throws Exception
     */
    public static void DelayProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        producer.setNamesrvAddr("localhost:9876;localhost:9877");
        producer.start();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("base", "tag1", ("Hello World " + i).getBytes());
            // 设置延迟级别
            message.setDelayTimeLevel(4);
            SendResult result = producer.send(message);
            System.out.println(result.getSendStatus() + "----" + result.getMsgId());
        }
        producer.shutdown();
    }
}
