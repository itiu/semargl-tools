package gost19.messaging.transport;

import java.util.List;

public interface IMessagingManager
{

    public void sendMessage(String to, String message, String queueToListen);

    public List<String> sendRequest(String uid, String to, String message,
            boolean withWaitingLimit, String queueToListen) throws RuntimeException;

    public String getMessage(String queueToListen, int waitingTime);

    public void close();
}

