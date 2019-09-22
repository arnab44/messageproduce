package com.message.producer;



import com.tcpmanager.BrokerClient.BrokerClient;
import com.tcpmanager.Message.Header;
import com.tcpmanager.Message.Message;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
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
    private static  ObjectOutputStream os;
    private Socket socket;
    public Producer(String path) {
        this.path = path;
        try {
            socket = new Socket("127.0.0.1", 5002);
            os= new ObjectOutputStream(socket.getOutputStream());
           // this.brokerClient = BrokerClient.getBrokerClient();
           // os = brokerClient.getObjectOutputStream();

            System.out.println("Connected to Message broker");
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
    }

    // constructor to put ip address and port
    public void readAndSendMessages()
    {
        System.out.println("Producer started at "+ new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()));
        List<Message> messageList = null;
        try (Stream<Path> walk = Files.walk(Paths.get(this.path))) {

            /*messageList =*/ walk.filter(Files::isRegularFile)
                    .map(x -> x.toAbsolutePath()).map(Producer::filetoMessage).forEach(Producer::sendMessage);//collect(Collectors.toList());
        }
        catch(IOException e) {
            System.out.println(e.toString());
        }
        //Collections.sort(messageList);
        Message lastMessage = Message.builder()
                .header(Header.builder()
                        .size(-1)
                        .build())
                .build();

        //messageList.add(lastMessage);
        sendMessage(lastMessage);

        //sending messages
       /* for (Message message :messageList)
        {
            try
            {
             //   System.out.println(message.getHeader().getFileName());
                os.writeObject(message);
                os.flush();
            }
            catch(IOException i)
            {
                System.out.println(i);
            }
        }*/

        // close the connection
        try
        {
            socket.close();
            os.close();
//          brokerClient.getSocket().close();
  //        brokerClient.getObjectOutputStream().close();
        }
        catch(IOException i)
        {
            System.out.println(i);
        }
        System.out.println("Producer finished at "+ new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()));
    }

        public static Message filetoMessage(Path p) {
        String contents = null;

        try {

            contents = new String(Files.readAllBytes(p));

        } catch (IOException ex) {
            System.out.println("Error while reading file " + ex.toString());
        }
        finally {


            return Message.builder().header(
                    Header.builder()
                            .size(contents.length())
                            .fileName(p.getFileName().toString())
                            .build())
                    .payLoad(contents)
                    .build();
        }
    }

    public  static boolean sendMessage(Message message) {
        try
        {
        //    System.out.println(message.getHeader().getFileName());
            os.writeObject(message);
            os.flush();
            return true;
        }
        catch(IOException e)
        {
            System.out.println("Exception while sending message "+e);
        }
        return true;
    }

}
