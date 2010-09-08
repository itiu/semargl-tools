package gost19.messaging.transport;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.MessageProperties;
import gost19.Predicates;
import gost19.messaging.MessageParser;
import gost19.messaging.TripleUtils;

public class AMQPMessagingManager implements IMessagingManager
{

    class Log
    {

        void trace(String msg)
        {
            System.out.println(msg);
        }

        void info(String msg)
        {
            System.out.println(msg);
        }

        void debug(String msg)
        {
            System.out.println(msg);
        }

        void error(String msg, Exception err)
        {
            System.out.println(msg + ", Exception:" + err.getMessage());
        }

        void error(String msg)
        {
            System.out.println(msg);
        }

        void warning(String msg)
        {
            System.out.println(msg);
        }
    }
    private Connection sendConnection;
    private String host;
    private int port;
    private String virtualHost;
    private String login;
    private String password;
    private long responceWaitingLimit = 0;
    private Log log;
    private TripleUtils tripleUtils = new TripleUtils();
    private MessageParser messageParser = new MessageParser();
    private Channel channel = null;
    private String requestQueueName = "request-queue-" + java.util.UUID.randomUUID().toString();
    private QueueingConsumer requestConsumer = null;
    private Object lock = new Object();
    private boolean isPersistenceMode = true;
    private Map<String, QueueingConsumer> consumersMap = new HashMap<String, QueueingConsumer>();

    public AMQPMessagingManager()
    {
    }

    public void init(String host, Integer port, String virtualHost, String userName, String password, long responceWaitingLimit)
    {
        this.host = host;
        this.port = port;
        this.virtualHost = virtualHost;
        this.login = userName;
        this.password = password;
        this.responceWaitingLimit = responceWaitingLimit;
        this.log = new Log();
    }

    /**
     * {@inheritDoc}
     *
     * @throws IOException
     */
    public void sendMessage(String to, String message, String queueToListen)
    {
        try
        {
//            String uid = java.util.UUID.randomUUID().toString();
            log.debug(String.format("[%s] Отправка в очередь '%s' сообщения \n %s", requestQueueName, to, message));

//            getChannel().queueDeclare(to, isPersistenceMode);
            if (queueToListen.length() > 0)
            {
                getChannel().queueDeclare(queueToListen, false, false, false, null);
            }

            if (isPersistenceMode)
            {
                getChannel().basicPublish("", to, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
            } else
            {
                getChannel().basicPublish("", to, MessageProperties.TEXT_PLAIN, message.getBytes());
            }

        } catch (IOException e)
        {
            log.error(String.format("[%s] Ошибка отправки сообщенния ", requestQueueName), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public List<String> sendRequest(String uid, String to, String message,
            boolean withWaitingLimit, String queueToListen) throws RuntimeException
    {
        requestQueueName = queueToListen;
        long start = System.nanoTime();
        log.debug(String.format("[%s] SEND_REQUEST [%s] : START : Подготовлен пакет для получателя '%s', isPersistenceMode = %s. Содержимое \n[\n%s\n]\n",
                requestQueueName, uid, to, isPersistenceMode, message));

        List<String> result = new ArrayList();

        try
        {
            getChannel().queueDeclare(requestQueueName, isPersistenceMode, false, true, null);

//            String setFromUid = java.util.UUID.randomUUID().toString();
//            message = String.format("%s<%s><%s><%s>.<%s><%s>\"%s\".<%s><%s>\"%s\".", message, setFromUid, Predicates.SUBJECT, Predicates.SET_FROM,
//                    setFromUid, Predicates.FUNCTION_ARGUMENT, requestQueueName, uid, Predicates.REPLY_TO, requestQueueName);

            // отправляем пакет
            if (isPersistenceMode)
            {
                getChannel().basicPublish("", to, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
            } else
            {
                getChannel().basicPublish("", to, MessageProperties.TEXT_PLAIN, message.getBytes());
            }

            log.debug(String.format("[%s] SEND_REQUEST [%s] : Пакет отправлен. Время ожидания ответа %s",
                    requestQueueName, uid, String.valueOf(responceWaitingLimit)));

            Delivery delivery = null;
//            if (requestConsumer == null || requestConsumer.getChannel() == null || !requestConsumer.getChannel().isOpen())
            {
                requestConsumer = new QueueingConsumer(getChannel());
                getChannel().basicConsume(requestQueueName, !isPersistenceMode, requestConsumer);
            }

            long startWaiting = System.nanoTime();
            boolean isStatusOk = false;
            boolean isStatusError = false;
            while (!(isStatusOk || isStatusError))
            {
                if ((System.nanoTime() - startWaiting) / 1000000 >= responceWaitingLimit)
                {
                    break;
                }

                try
                {
                    log.debug(String.format("[%s] request consumer.nextDelivery start", uid));
                    delivery = requestConsumer.nextDelivery(responceWaitingLimit);
                    log.debug(String.format("[%s] request consumer.nextDelivery end", uid));
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }

                if (delivery != null)
                {
                    String r = new String(delivery.getBody());
                    log.debug(String.format("[%s] SEND_REQUEST [%s] : Получено сообщение : %s", requestQueueName, uid, r));

                    List<String> replyTriples = messageParser.split(r);
                    if (replyTriples.size() > 0)
                    {
                        String replyUid = tripleUtils.getTripleFromLine(replyTriples.get(0)).getSubj();
                        if (replyUid.equals(uid))
                        {
                            String status = tripleUtils.getStatusFromReply(r);
                            log.debug(String.format("[%s] SEND_REQUEST [%s] : ...статус : %s", requestQueueName, uid, status));
                            if (status != null)
                            {
                                if (status.equals(Predicates.STATE_OK))
                                {
                                    isStatusOk = true;
                                } else if (status.equals(Predicates.STATE_ERROR))
                                {
                                    isStatusError = true;
                                }
                            }
                            else
                            {
                                result.addAll(tripleUtils.getDataFromReply(r));
                            }

                            log.debug(String.format("[%s] SEND_REQUEST [%s] : ...isStatusOk : %s", requestQueueName, uid, isStatusOk));
                            if (isStatusOk)
                            {
                                result.addAll(tripleUtils.getDataFromReply(r));
                                log.debug(String.format("[%s] SEND_REQUEST [%s] : ...result : %s", requestQueueName, uid, result));
                            }

                        } else
                        {
                            log.warning(String.format("[%s] SEND_REQUEST [%s] : ...INVALID ANSWER UID! WAITING NEXT!", requestQueueName, uid));
                            startWaiting += responceWaitingLimit;
                        }
                    } else
                    {
                        log.warning(String.format("[%s] SEND_REQUEST [%s] : ...ANSWER PARSING ERROR! WAITING NEXT!", requestQueueName, uid));
                        startWaiting += responceWaitingLimit;
                    }
                    if (isPersistenceMode)
                    {
                        try
                        {
                            getChannel().basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                        } catch (IOException e)
                        {
                            log.error(String.format("[%s] SEND_REQUEST [%s] : Ошибка уведомления о получении.", requestQueueName, uid));
                            e.printStackTrace();
                        }
                        log.debug(String.format("[%s] SEND_REQUEST [%s] : ...уведомление отправлено", requestQueueName, uid));
                    }
                }
            }

            getChannel().queueDelete(requestQueueName);

            if (result.size() > 0)
            {
                log.debug(String.format("[%s] SEND_REQUEST [%s] : Получен результат : %s", requestQueueName, uid, result.toString()));
            } else
            {
                if (isStatusOk)
                {
                    log.info(String.format("[%s] SEND_REQUEST [%s] : Получен пустой результат.", requestQueueName, uid));
                } else
                {
                    log.error(String.format("[%s] SEND_REQUEST [%s] : Результат не получен в течение заданного времени ожидания.", requestQueueName, uid));
                }
            }

        } catch (IOException e)
        {
            log.error(String.format("[%s] SEND_REQUEST [%s] : Ошибка отправки запроса.", requestQueueName, uid));
            log.error(String.format("[%s] %s", requestQueueName, e.getMessage()));
        }

        long finish = System.nanoTime();
        double duration = (finish - start) / 1000000;
        String traceMessage = String.format("[%s] SEND_REQUEST [%s] : FINISH : Запрос выполнен за %5.2f msec.", requestQueueName, uid, duration);
        log.trace(String.format("[%s] %s", requestQueueName, traceMessage));
        return result;

    }

    /**
     * {@inheritDoc}
     */
    public String getMessage(String queueToListen, int waitingTime)
    {

        long start = System.nanoTime();

        String result = null;

        String uid = java.util.UUID.randomUUID().toString();

        try
        {
//            getChannel().queueDeclare(queueToListen, isPersistenceMode);
            getChannel().queueDeclare(queueToListen, false, false, false, null);
        } catch (Exception e)
        {
            log.error(String.format("[%s] Проверка очереди [%s] на предмет нового сообщения. Время ожидания = %d мсек.",
                    requestQueueName, queueToListen, waitingTime));
            log.error(String.format("GET_MESSAGE [%s] Ошибка создания очереди для приема сообщения.", uid), e);
            return null;
        }

        QueueingConsumer consumer = consumersMap.get(queueToListen);
        if (consumer == null || consumer.getChannel() == null || !consumer.getChannel().isOpen())
        {
            consumer = new QueueingConsumer(getChannel());
            try
            {
                getChannel().basicConsume(queueToListen, !isPersistenceMode, consumer);
                consumersMap.put(queueToListen, consumer);
            } catch (Exception e)
            {
                log.error("Ошибка при маппинге потребителя для очереди " + queueToListen, e);
            }
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
                if (isPersistenceMode)
                {
                    getChannel().basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
                result = new String(delivery.getBody());
            }
        } catch (Exception e)
        {
            log.debug(String.format("[%s] Проверка очереди [%s] на предмет нового сообщения. Время ожидания = %d мсек.",
                    requestQueueName, queueToListen, waitingTime));
            log.error(String.format("GET_MESSAGE [%s] Ошибка приема AMQP пакета.", uid), e);
            return null;
        }

        if (result != null)
        {
            log.debug(String.format("[%s] GET_MESSAGE [%s] Получено сообщение : %s", requestQueueName, uid, result));
        }

        return result;
    }

    private Channel getChannel()
    {
        while (channel == null || !channel.isOpen())
        {
            try
            {
                channel = getConnection().createChannel();
            } catch (IOException e)
            {
                log.error(String.format("[%s] Ошибка открытия AMQP канала.\n%s", requestQueueName, e));
                try
                {
                    Thread.sleep(500);
                } catch (InterruptedException ex)
                {
                    log.error(String.format("[%s] %s", requestQueueName, ex.getMessage()));
                }
                continue;
            }
            break;
        }
        return channel;
    }

    /**
     * Возвращает соединение.
     *
     * @return
     */
    private Connection getConnection() throws RuntimeException
    {
        if (sendConnection != null && !sendConnection.isOpen())
        {
            sendConnection = null;
        }
        while (sendConnection == null)
        {
            System.out.print(String.format("[%s] Попытка соединения с сервером AMQP : host = %s, port = %s, "
                    + "virtualHost = %s, login = %s, password = %s",
                    requestQueueName, host, port, virtualHost, login, password));

            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setVirtualHost(virtualHost);
            factory.setUsername(login);
            factory.setPassword(password);

            try
            {
                sendConnection = factory.newConnection();
                log.debug(String.format("[%s] Соединение создано успешно.", requestQueueName));
            } catch (IOException e)
            {
                log.error(String.format("[%s] Ошибка создания AMQP соединения.", requestQueueName));
                log.error(String.format("[%s] %s", requestQueueName, e.getMessage()));
                try
                {
                    Thread.sleep(500);
                } catch (InterruptedException ex)
                {
                    ex.printStackTrace();
                }
                continue;
            }
            break;
        }
        return sendConnection;
    }

    public void setPersistenceMode(boolean isPersistenceMode)
    {
        this.isPersistenceMode = isPersistenceMode;
    }

    public void close()
    {
        try
        {
            if (sendConnection != null && sendConnection.isOpen())
            {
                sendConnection.close();
            }
        } catch (Exception e)
        {
            log.error("Ошибка закрытия AMQP-соединения", e);
        }
    }
}
