package com.example.demo.trans;

import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * @ClassName Producer
 * @Description
 * @Author Rex
 * @Date 2020-09-24 12:55
 * @Version 1.0
 **/
public class Producer {

    public static void main(String[] args) throws Exception {
        TransProducer();
    }

    /**
     * 发送事务消息,与本地事务处于同一事务状态下,保证强一致性
     *
     * @throws Exception
     */
    public static void TransProducer() throws Exception {
        // 创建事务生产者
        TransactionMQProducer producer = new TransactionMQProducer("group2");
        // 设置 namesrv 地址
        producer.setNamesrvAddr("localhost:9876;localhost:9877");
        // 设置事务监听器
        producer.setTransactionListener(new TransactionListener() {
            // 执行本地事务,并设置消息提交或回滚
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                if (message.getTags().equals("TagA")) {
                    System.out.println("本地事务处理成功!!");
                    return LocalTransactionState.COMMIT_MESSAGE;
                } else if (message.getTags().equals("TagB")) {
                    System.out.println("本地事务处理失败!!");
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                } else if (message.getTags().equals("TagC")) {
                    System.out.println("本地事务处理结果未知!!");
                    return LocalTransactionState.UNKNOW;
                }
                return LocalTransactionState.UNKNOW;
            }

            // 事务回查
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                System.out.println("执行事务回查成功:" + messageExt.getTags());
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });

        producer.start();
        String[] tags = {"TagA", "TagB", "TagC"};
        for (int i = 0; i < 3; i++) {
            Message message = new Message("base", tags[i], ("Hello World " + i).getBytes());
            // 发送事务消息
            SendResult result = producer.sendMessageInTransaction(message, null);
            System.out.println(result.getSendStatus() + "----" + result.getMsgId() + "----" + result.getMessageQueue().getQueueId());
        }
        //producer.shutdown();
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
