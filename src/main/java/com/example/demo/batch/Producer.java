package com.example.demo.batch;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;

/**
 * @ClassName Producer
 * @Description
 * @Author Rex
 * @Date 2020-09-24 12:55
 * @Version 1.0
 **/
public class Producer {

    public static void main(String[] args) throws Exception {
        BatchProducer();
    }

    /**
     * 发送同步消息
     *
     * @throws Exception
     */
    public static void BatchProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        producer.setNamesrvAddr("localhost:9876;localhost:9877");
        producer.start();
        // 对于批量消息,单次消息不能大于 4M
        ArrayList<Message> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("base", "tag1", ("Hello World " + i).getBytes());
            list.add(message);
        }
        SendResult result = producer.send(list);
        producer.shutdown();
    }
}
