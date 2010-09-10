package gost19.messaging.transport;

import gost19.Predicates;
import java.util.List;

import gost19.messaging.MessageParser;
import gost19.messaging.TripleUtils;
import java.io.IOException;
import java.util.ArrayList;
import org.zeromq.ZMQ;

public class ZMQMessagingManager implements IMessagingManager
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
    private Log log;
    private TripleUtils tripleUtils = new TripleUtils();
    private MessageParser messageParser = new MessageParser();
    private ZMQ.Socket s;

    public ZMQMessagingManager()
    {
    }

    public void init(String point)
    {
        this.log = new Log();
        ZMQ.Context ctx = ZMQ.context(1);
        s = ctx.socket(ZMQ.REQ);
        log.info("ZMQ:connect to: " + point);
        s.connect(point);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IOException
     */
    public void sendMessage(String to, String message, String queueToListen) throws Exception
    {
        throw new Exception("not implemented");
    }

    /**
     * {@inheritDoc}
     */
    public List<String> sendRequest(String uid, String to, String message,
            boolean withWaitingLimit, String queueToListen) throws RuntimeException
    {
        boolean isStatusOk = false;
        boolean isStatusError = false;
        List<String> result = new ArrayList();

        byte[] bb = (message + "\0").getBytes();
        s.send(bb, 0);

        boolean first_loop = true;

        while (!(isStatusOk || isStatusError))
        {
            byte data[];

            if (first_loop)
            {
                data = s.recv(0);
            } else
            {
                data = s.recv(ZMQ.RCVMORE);
            }

            first_loop = false;

            String r = new String(data);

            List<String> replyTriples = messageParser.split(r);
            if (replyTriples.size() > 0)
            {
                String replyUid = tripleUtils.getTripleFromLine(replyTriples.get(0)).getSubj();
                if (replyUid.equals(uid))
                {
                    String status = tripleUtils.getStatusFromReply(r);
//                log.debug(String.format("[%s] SEND_REQUEST [%s] : ...статус : %s", requestQueueName, uid, status));
                    if (status != null)
                    {
                        if (status.equals(Predicates.STATE_OK))
                        {
                            isStatusOk = true;
                        } else if (status.equals(Predicates.STATE_ERROR))
                        {
                            isStatusError = true;
                        }
                    } else
                    {
                        result.addAll(tripleUtils.getDataFromReply(r));
                    }

//                log.debug(String.format("[%s] SEND_REQUEST [%s] : ...isStatusOk : %s", requestQueueName, uid, isStatusOk));
                    if (isStatusOk)
                    {
                        result.addAll(tripleUtils.getDataFromReply(r));
//                    log.debug(String.format("[%s] SEND_REQUEST [%s] : ...result : %s", requestQueueName, uid, result));
                    }

                } else
                {
                    //               log.warning(String.format("[%s] SEND_REQUEST [%s] : ...INVALID ANSWER UID! WAITING NEXT!", requestQueueName, uid));
//                startWaiting += responceWaitingLimit;
                }
            } else
            {
            }
        }

        return result;
    }

    /**
     * {@inheritDoc}
     */
    public String getMessage(String queueToListen, int waitingTime) throws Exception
    {
        throw new Exception("not implemented");
    }

    public void close()
    {
    }
}

