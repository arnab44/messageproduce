package com.message.producer;



import com.tcpmanager.BrokerClient.BrokerClient;
import com.tcpmanager.Message.Header;
import com.tcpmanager.Message.Message;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


class Pair implements Comparable {
    public long t;
    public File f;

    public Pair(File file) {
        f = file;
        t = file.length();
    }

    public int compareTo(Object o) {
        long u = ((Pair) o).t;
        return t < u ? -1 : t == u ? 0 : 1;
    }
};


public class Producer
{
    String path = null;
    private BrokerClient brokerClient;
    private ObjectOutputStream os;
    public Producer(String path) {
        this.path = path;
        try {
            this.brokerClient = BrokerClient.getBrokerClient();
            os = brokerClient.getObjectOutputStream();

            System.out.println("Connected to Message broker");
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
    }

    // constructor to put ip address and port
    public void readAndSendMessages()
    {
        List<Message> messageList = null;
        try (Stream<Path> walk = Files.walk(Paths.get(this.path))) {

            messageList = walk.filter(Files::isRegularFile)
                    .map(x -> x.toAbsolutePath()).map(Producer::filetoMessage).collect(Collectors.toList());
        }
        catch(IOException e) {
            System.out.println(e.toString());
        }
        Collections.sort(messageList);
        Message lastMessage = Message.builder()
                .header(Header.builder()
                        .size(-1)
                        .build())
                .build();

        messageList.add(lastMessage);

        //sending messages
        for (Message message :messageList)
        {
            try
            {
                os.writeObject(message);
                os.flush();
            }
            catch(IOException i)
            {
                System.out.println(i);
            }
        }

        // close the connection
        try
        {
          brokerClient.getSocket().close();
          brokerClient.getObjectOutputStream().close();
        }
        catch(IOException i)
        {
            System.out.println(i);
        }
    }

        public static Message filetoMessage(Path p) {
        String contents = null;

        try {

            contents = new String(Files.readAllBytes(p));

        } catch (IOException ex) {
            System.out.println("Error while reading file " + ex.toString());
        }
        return Message.builder().header(
                Header.builder()
                        .size(contents.length())
                        .fileName(p.getFileName().toString())
                        .build())
                .payLoad(contents)
                .build();
    }

}
