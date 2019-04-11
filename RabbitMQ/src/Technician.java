import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;

public class Technician {

    private final BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    private String name;
    private final List<String> specs = new ArrayList<>();
    private Connection connection;

    public Technician() throws Exception {

        System.out.println("Enter name: ");
        name = br.readLine();

        System.out.println("Enter first body part: ");
        String first = br.readLine();
        specs.add(first);

        System.out.println("Enter second body part: ");
        String second = br.readLine();
        specs.add(second);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();

        //specs listener queues
        specs.forEach((exam) -> {
            String key = "exam." + exam;
            try {
                Channel channel = connection.createChannel();
                channel.basicQos(1);
                channel.queueDeclare(key, false, false, false, null);
                channel.basicConsume(key, false, new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                        String message = new String(body, "UTF-8");
                        System.out.println("Received: " + message);

                        try {
                            Random rand = new Random();
                            int value = rand.nextInt(5) + 1;
                            Thread.sleep(value * 1000);

                            String[] msgParts = message.split("\\.");
                            String doctorName  = msgParts[0];
                            String patientName = msgParts[1];
                            String examination = msgParts[2];

                            Channel responseChannel = connection.createChannel();
                            responseChannel.queueDeclare(doctorName, false, false, false, null);

                            channel.basicAck(envelope.getDeliveryTag(), false);
                            String responseMessage =  patientName + "." + examination + ".done";
                            responseChannel.basicPublish("", doctorName, null, responseMessage.getBytes());
                            System.out.println("Sent " + responseMessage);

                            responseChannel.close();

                        } catch (TimeoutException | InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            }

        });

        System.out.println("Waiting for examinations...");

    }

    public static void main(String[] args) throws Exception {

        new Technician();
    }
}