import com.message.producer.Producer;

public class MessageProducerApplication {


    public static  void main(String[] args) {
        String  path  = "/Users/arnabs/Desktop/sampleData";
      //  String path = "/home/ec2-user/sampleData";
      //  String path = "/data/finalDataSet4Sept2019/";
        Producer producer = new Producer(path);
        producer.readAndSendMessages();
    }
}
