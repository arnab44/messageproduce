import com.message.producer.Producer;

public class MessageProducerApplication {


    public static  void main(String[] args) {
        String  path  = "/Users/arnabs/Desktop/sampleData";
        Producer producer = new Producer(path);
        producer.readAndSendMessages();
    }
}
