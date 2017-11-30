package topictests;

import org.junit.Test;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TopicNonDurableTests extends TestBase {

    @Test
    public void testSharedNonDurableSubscription() throws JMSException, NamingException, InterruptedException, ExecutionException, TimeoutException {
        int iterations = 1;
        for (int i = 0; i < iterations; i++) {
            System.out.println("testSharedNonDurableSubscription; iteration: " + i);
            //SETUP-START
            Hashtable env = new Hashtable<Object, Object>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
            env.put("connectionfactory.qpidConnectionFactory", "amqp://localhost:5672");
            env.put("topic." + "jmsTopic", "jmsTopic");
            Context context1 = new InitialContext(env);
            ConnectionFactory connectionFactory1 = (ConnectionFactory) context1.lookup("qpidConnectionFactory");
            Connection connection1 = connectionFactory1.createConnection();


            Hashtable env2 = new Hashtable<Object, Object>();
            env2.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
            env2.put("connectionfactory.qpidConnectionFactory", "amqp://localhost:5672");
            env2.put("topic." + "jmsTopic", "jmsTopic");
            Context context2 = new InitialContext(env2);
            ConnectionFactory connectionFactory2 = (ConnectionFactory) context2.lookup("qpidConnectionFactory");
            Connection connection2 = connectionFactory2.createConnection();

            connection1.start();
            connection2.start();

            Session session = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic testTopic = (Topic) context1.lookup("jmsTopic");
            //SETUP-END

            //BODY-S
            String subID = "sharedConsumerNonDurable123";
            MessageConsumer subscriber1 = session.createSharedConsumer(testTopic, subID);
            MessageConsumer subscriber2 = session2.createSharedConsumer(testTopic, subID);
            MessageConsumer subscriber3 = session2.createSharedConsumer(testTopic, subID);
            MessageProducer messageProducer = session.createProducer(testTopic);
            messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            int count = 10;
            List<Message> listMsgs = generateMessages(session, count);
            List<CompletableFuture<List<Message>>> results = receiveMessagesAsync(count, subscriber1, subscriber2, subscriber3);
            sendMessages(messageProducer, listMsgs);
            System.out.println("messages sent");

            assertThat("Each message should be received only by one consumer",
                    results.get(0).get(20, TimeUnit.SECONDS).size() +
                            results.get(1).get(20, TimeUnit.SECONDS).size() +
                            results.get(2).get(20, TimeUnit.SECONDS).size(),
                    is(count));
            System.out.println("messages received");
            //BODY-E

            Thread.sleep(20000);
            //TEAR-DOWN-S
            connection1.stop();
            connection2.stop();
            subscriber1.close();
            subscriber2.close();
            session.close();
            session2.close();
            connection1.close();
            connection2.close();
            //TEAR-DOWN-E
        }
    }

    @Test
    public void testTopicWildcards() throws Exception {


        //SETUP-START
        Hashtable env = new Hashtable<Object, Object>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
        env.put("connectionfactory.qpidConnectionFactory", "amqp://localhost:5672");


        int msgCount = 1000;
        int topicCount = 10;
        int senderCount = topicCount;
        int recvCount = topicCount / 2;

        //create topics
        for (int i = 0; i < recvCount; i++) {
            env.put("topic." + String.format("test-topic-pubsub%d.%d", i, i + 1), String.format("test-topic-pubsub%d.%d", i, i + 1));
            env.put("topic." + String.format("test-topic-pubsub%d.%d", i, i + 2), String.format("test-topic-pubsub%d.%d", i, i + 2));
        }
        Context context1 = new InitialContext(env);
        ConnectionFactory connectionFactory1 = (ConnectionFactory) context1.lookup("qpidConnectionFactory");
        Connection connection1 = connectionFactory1.createConnection();

        connection1.start();

        Session session = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

        List<Topic> topicList = new ArrayList<>();
        for (int i = 0; i < recvCount; i++) {
            topicList.add((Topic) context1.lookup(String.format("test-topic-pubsub%d.%d", i, i + 1)));
            topicList.add((Topic) context1.lookup(String.format("test-topic-pubsub%d.%d", i, i + 2)));
        }
        //SETUP-END

        //BODY-S

        //attach subscibers
        List<MessageConsumer> consumers = new ArrayList<>();
        for (int i = 0; i < recvCount; i++) {
            consumers.add(session.createConsumer(session.createTopic(String.format("test-topic-pubsub%d.*", i))));
        }

        List<List<CompletableFuture<List<Message>>>> consResults = new ArrayList<>();
        for (MessageConsumer c : consumers) {
            consResults.add(receiveMessagesAsync(msgCount * 2, c));
        }

        //attach producers
        List<MessageProducer> producers = new ArrayList<>();
        for (int i = 0; i < senderCount; i++) {
            producers.add(session.createProducer(topicList.get(i)));

        }

        List<Message> msgBatch = generateMessages(session, msgCount);

        for (MessageProducer p : producers) {
            sendMessages(p, msgBatch);
            System.out.println("messages sent: " + producers.toString());
        }


        //check received messages
        for (List<CompletableFuture<List<Message>>> l : consResults) {
            for (CompletableFuture<List<Message>> l2 : l) {
                assertThat(l2.get(20, TimeUnit.SECONDS).size(), is(msgCount * 2));
                System.out.println("messages received");
            }
        }
        //BODY-E

        Thread.sleep(20000);
        //TEAR-DOWN-S
        connection1.stop();


        for (MessageProducer p : producers) {
            p.close();
            System.out.println("producer closed: " + producers.toString());
        }
        for (MessageConsumer c : consumers) {
            c.close();
            System.out.println("consumer closed: " + c.toString());
        }

        session.close();
        connection1.close();
        //TEAR-DOWN-E

    }

}
