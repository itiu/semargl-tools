package gost19.amqp.messaging;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConnectionParameters;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.QueueingConsumer.Delivery;
//import com.rabbitmq.client.GetResponse;

public class AMQPMessagingManager
{
    //AMQP

    private Connection connection;
    private String host;
    private int port;
    private String virtualHost;
    private String login;
    private String password;
    private long responceWaitingLimit = 0;
    private Channel channel;
    private Channel channel_get_message = null;
    private QueueingConsumer consumer = null;

    public AMQPMessagingManager()
    {
    }

    synchronized public void init(String host, Integer port, String virtualHost, String userName, String password,
            long responceWaitingLimit) throws Exception
    {
        this.host = host;
        this.port = port;
        this.virtualHost = virtualHost;
        this.login = userName;
        this.password = password;
        this.responceWaitingLimit = responceWaitingLimit;


        getConnection();

        channel = getConnection().createChannel();
        channel.queueDeclare("semargl-test", true);
    }

    /**
     * {@inheritDoc}
     * 
     * @throws IOException
     */
    synchronized public void sendMessage(String to, String message, String callback_queue)
    {
        try
        {
            //	    long start = System.nanoTime();

//            channel = getConnection().createChannel();
//	    channel.close();
//            channel = getConnection().createChannel();
//            channel.queueDeclare(callback_queue);
//	    channel.queueDeclare(to);

//            String uid = java.util.UUID.randomUUID().toString();
            //	    System.out.println(String.format("[%s] Отправка в очередь '%s' сообщения \n %s", uid, to, message));

            BasicProperties props = new BasicProperties();
            channel.basicPublish("", to, props, message.getBytes());
//	    channel.close();

            /*	    if (log.isTraceEnabled()) {
            long finish = System.nanoTime();
            double duration = (finish - start) / 1000000;
            String traceMessage = String.format("[%s] Отправлено за %5.2f msec.", uid, duration);
            log.trace(traceMessage);
            }*/

        } catch (IOException e)
        {
            e.printStackTrace();
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }


    /**
     * {@inheritDoc}
     */
    synchronized public String getMessage(String queueToListen, int waitingTime)
    {

        long start = System.nanoTime();

        String result = null;

        //		log
        //				.debug(String
        //						.format(
        //								"Проверка очереди [%s] на предмет нового сообщения. Время ожидания = %d мсек.",
        //								queueToListen, waitingTime));
        channel_get_message = null;

        String uid = java.util.UUID.randomUUID().toString();
        Channel channel = null;
        try
        {
            if (channel_get_message == null)
            {
                channel = getConnection().createChannel();
                channel_get_message = channel;

                try
                {
                    channel.queueDeclare(queueToListen);
                } catch (IOException e)
                {
                    throw new RuntimeException(String.format("GET_MESSAGE [%s] Ошибка создания очереди для приема сообщения.", uid), e);
                }
                consumer = new QueueingConsumer(channel);
                channel.basicConsume(queueToListen, consumer);

            } else
            {
                channel = channel_get_message;
            }
        } catch (Exception e)
        {
            throw new RuntimeException(String.format("GET_MESSAGE [%s] Ошибка создания канала AMQP.", uid), e);
        }

        try
        {
            Delivery delivery;
            if (waitingTime > 0)
            {
                delivery = consumer.nextDelivery(waitingTime);
            } else
            {
                delivery = consumer.nextDelivery();
            }
            if (delivery != null)
            {
                               channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                result = new String(delivery.getBody());
            }

        channel.close();
        
        } catch (Exception e)
        {
            throw new RuntimeException(String.format("GET_MESSAGE [%s] Ошибка приема AMQP пакета.", uid), e);
        }

//	if (result != null) {
//	    System.out.println(String.format("GET_MESSAGE [%s] Получено сообщение : %s", uid, result));
//	} else {
//	    log.debug("Нет новых сообщений.");
//	}
/*
        try {
        channel.close();
        } catch (IOException e) {
        System.out.println(String.format("GET_MESSAGE [%s] Ошибка закрытия AMQP канала.", uid));
        e.printStackTrace();
        }
         */
        /*	if (result != null) {
        long finish = System.nanoTime();
        double duration = (finish - start) / 1000000;
        String traceMessage = String.format("GET_MESSAGE [%s] Метод отработал %5.2f msec.", uid, duration);
        System.out.println(traceMessage);
        }*/
        return result;
    }

    /**
     * Возвращает соединение.
     * 
     * @return
     */
    private Connection getConnection() throws RuntimeException
    {
        if (connection == null || !connection.isOpen())
        {

            System.out.println(String.format(
                    "Попытка соединения с сервером AMQP : host = %s, port = %s, "
                    + "virtualHost = %s, login = %s, password = %s",
                    host, port, virtualHost, login, password));

            ConnectionParameters params = new ConnectionParameters();
            params.setUsername(login);
            params.setPassword(password);
            params.setVirtualHost(virtualHost);
            params.setRequestedHeartbeat(0);

            ConnectionFactory factory = new ConnectionFactory(params);
            try
            {
                connection = factory.newConnection(host, port);
            } catch (IOException e)
            {
                System.out.println("Ошибка установки AMQP соединения.");
                throw new RuntimeException("Ошибка установки AMQP соединения.", e);
            }

            System.out.println("Соединение создано успешно.");

        }
        return connection;
    }
}
