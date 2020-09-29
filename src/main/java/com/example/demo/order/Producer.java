package com.example.demo.order;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * @ClassName Producer
 * @Description
 * @Author Rex
 * @Date 2020-09-24 12:55
 * @Version 1.0
 **/
public class Producer {

    public static void main(String[] args) throws Exception {
        OrderProducer();
    }

    /**
     * 顺序消息:同一Topic的消息存储在不同的 MessageQueue 中,同一 MessageQueue 中的消息是顺序的,
     * 实现顺序消息,就是将顺序消息存储在同一 MessageQueue 中
     *
     * @throws Exception
     */
    public static void OrderProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        producer.setNamesrvAddr("localhost:9876;localhost:9877");
        producer.start();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("base", "tag1", ("Hello World " + i).getBytes());
            // 消息,messageQueeu选择器,业务参数
            SendResult result = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    int id = (int) o;
                    int index = id % list.size();
                    return list.get(index);
                }
            }, i);
            System.out.println(result + "-----" + i);
        }
        producer.shutdown();
    }
}
