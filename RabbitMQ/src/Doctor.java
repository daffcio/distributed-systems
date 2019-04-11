import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;


public class Doctor {

    private final BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    private String name;
    private Connection connection;
    private Channel responseListener;
    private Map<String, Channel> examChannels = new HashMap<>();


    public Doctor() throws Exception {

        System.out.println("Enter name: ");
        name = br.readLine();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();

        //response listener queue
        responseListener = connection.createChannel();
        responseListener.queueDeclare(name, false, false, false, null);
        responseListener.basicConsume(name, true, new DefaultConsumer(responseListener) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Response from technician: " + message);
            }
        });

        //exams writers queues
        List<String> examinations = Arrays.asList("hip", "knee", "elbow");
        examinations.forEach((exam) -> {
            String key = "exam." + exam;
            try {
                Channel channel = connection.createChannel();
                channel.queueDeclare(key, false, false, false, null);
                examChannels.put(exam, channel);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        sendLoop();
    }


    private void sendLoop() {
        System.out.println("Start sending examinations: ");
        while (true) {
            try {

                System.out.println("Enter patient name: ");
                String patient = br.readLine();

                if (patient.equals("quit")) {
                    break;
                }

                System.out.println("Enter examination type: ");
                String examination = br.readLine();

                if (examChannels.containsKey(examination)) {
                    String message = name + "."  + patient + "." + examination;
                    examChannels.get(examination).basicPublish("", "exam." + examination, null, message.getBytes());
                    System.out.println("Sent examination request: " + message);
                } else {
                    System.out.println("Bad examination type");
                    System.out.println("Usage:  [patient name] [examination type (hip, knee or elbow)]");
                }

                System.out.println("Next examination: ");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            responseListener.close();
            examChannels.forEach((key, channel) -> {
                try {
                    channel.close();
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            });

            connection.close();
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        try {
            new Doctor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

