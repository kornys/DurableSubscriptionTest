package topictests;

import org.junit.Test;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TopicDurableTests {

    @Test
    public void testMessageDurableSubscription() throws Exception {
        Hashtable env = new Hashtable<Object, Object>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
        StringBuilder urlParam = new StringBuilder();
        urlParam.append("jms.clientID=" + "jmsTopicClient");

        env.put("connectionfactory.qpidConnectionFactory", "amqp://localhost:5672" +  "?" + urlParam.toString());
        env.put("topic." + "jmsTopic", "jmsTopic");

        Context context = new InitialContext(env);
        ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("qpidConnectionFactory");
        Connection connection = connectionFactory.createConnection();
        connection.start();

        System.out.println("testMessageDurableSubscription");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic testTopic = (Topic) context.lookup("jmsTopic");

        String sub1ID = "sub1DurSub";
        String sub2ID = "sub2DurSub";
        MessageConsumer subscriber1 = session.createDurableSubscriber(testTopic, sub1ID);
        MessageConsumer subscriber2 = session.createDurableSubscriber(testTopic, sub2ID);
        MessageProducer messageProducer = session.createProducer(testTopic);

        int count = 100;
        String batchPrefix = "First";
        List<Message> listMsgs = generateMessages(session, batchPrefix, count);
        sendMessages(messageProducer, listMsgs);
        System.out.println("First batch messages sent");

        List<Message> recvd1 = receiveMessages(subscriber1, count);
        List<Message> recvd2 = receiveMessages(subscriber2, count);

        assertThat(recvd1.size(), is(count));
        assertMessageContent(recvd1, batchPrefix);
        System.out.println(sub1ID + " :First batch messages received");

        assertThat(recvd2.size(), is(count));
        assertMessageContent(recvd2, batchPrefix);
        System.out.println(sub2ID + " :First batch messages received");

        subscriber1.close();
        System.out.println(sub1ID + " : closed");

        batchPrefix = "Second";
        listMsgs = generateMessages(session, batchPrefix, count);
        sendMessages(messageProducer, listMsgs);
        System.out.println("Second batch messages sent");

        recvd2 = receiveMessages(subscriber2, count);
        assertThat(recvd2.size(), is(count));
        assertMessageContent(recvd2, batchPrefix);
        System.out.println(sub2ID + " :Second batch messages received");

        subscriber1 = session.createDurableSubscriber(testTopic, sub1ID);
        System.out.println(sub1ID + " :connected");

        recvd1 = receiveMessages(subscriber1, count);
        assertThat(recvd1.size(), is(count));
        assertMessageContent(recvd1, batchPrefix);
        System.out.println(sub1ID + " :Second batch messages received");

        subscriber1.close();
        subscriber2.close();

        session.unsubscribe(sub1ID);
        session.unsubscribe(sub2ID);
    }

    private void sendMessages(MessageProducer producer, List<Message> messages) {
        messages.forEach(m -> {
            try {
                producer.send(m);
            } catch (JMSException e) {
                e.printStackTrace();
            }
        });
    }

    protected List<Message> receiveMessages(MessageConsumer consumer, int count) {
        return receiveMessages(consumer, count, 0);
    }

    protected List<Message> receiveMessages(MessageConsumer consumer, int count, long timeout) {
        List<Message> recvd = new ArrayList<>();
        IntStream.range(0, count).forEach(i -> {
            try {
                recvd.add(timeout > 0 ? consumer.receive(timeout) : consumer.receive());
            } catch (JMSException e) {
                e.printStackTrace();
            }
        });
        return recvd;
    }

    protected void assertMessageContent(List<Message> msgs, String content) {
        msgs.forEach(m -> {
            try {
                assertTrue(((TextMessage) m).getText().contains(content));
            } catch (JMSException e) {
                e.printStackTrace();
            }
        });
    }

    protected List<Message> generateMessages(Session session, int count) {
        return generateMessages(session, "", count);
    }

    protected List<Message> generateMessages(Session session, String prefix, int count) {
        List<Message> messages = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        IntStream.range(0, count).forEach(i -> {
            try {
                messages.add(session.createTextMessage(sb.append(prefix).append("testMessage").append(i).toString()));
                sb.setLength(0);
            } catch (JMSException e) {
                e.printStackTrace();
            }
        });
        return messages;
    }
}