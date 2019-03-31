import java.util.Scanner;

public class Main {

    public static void main(String[] args) throws Exception{
        //System.setProperty("jgroups.bind_addr", "127.0.0.1");

        System.setProperty("java.net.preferIPv4Stack", "true");

        String channelName = "SRCHANNEL";
        String multicastAddress = "230.100.200.4";
        DistributedMap map = new DistributedMap(channelName,multicastAddress);

        Scanner scanner = new Scanner(System.in);
        String input;
        String key;
        int value;

        while(!(input = scanner.next()).equals("quit")){
            switch(input){
                case "contains":
                    key = scanner.next();
                    System.out.println(map.containsKey(key));
                    break;
                case "get":
                    key = scanner.next();
                    System.out.println(map.get(key));
                    break;
                case "put":
                    key = scanner.next();
                    value = scanner.nextInt();
                    map.put(key,value);
                    break;
                case "remove":
                    key = scanner.next();
                    System.out.println(map.remove(key));
                    break;
                case "state":
                    map.printState();
                    break;
                default:
                    System.out.println("Wrong operation - try again");
                    break;
            }
        }
        map.close();
    }
}
