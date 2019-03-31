import javafx.util.converter.IntegerStringConverter;
import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.*;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DistributedMap extends ReceiverAdapter implements SimpleStringMap {

    private final Map<String,Integer> map = new HashMap<>();
    private JChannel channel;
    private ProtocolStack stack;

    /*
    DistributedMap(String channelName){
        try{
            this.channel = new JChannel();
            channel.setReceiver(this);
            channel.connect(channelName);
            channel.getState(null, 30000);
        } catch(Exception e){
            e.printStackTrace();
        }
    }
    */

    DistributedMap(String channelName, String multicastAddress){
        try{
            this.channel = new JChannel(false);
            channel.setReceiver(this);
            initProtocolStack(multicastAddress);
            channel.connect(channelName);
            channel.getState(null, 30000);
        } catch(Exception e){
            e.printStackTrace();
        }
    }


    private void initProtocolStack(String multicastAddress) throws Exception{
        stack = new ProtocolStack();
        channel.setProtocolStack(stack);
        stack.addProtocol(new UDP().setValue("mcast_group_addr", InetAddress.getByName(multicastAddress)))
                .addProtocol(new PING())
                .addProtocol(new MERGE3())
                .addProtocol(new FD_SOCK())
                .addProtocol(new FD_ALL().setValue("timeout", 12000).setValue("interval", 3000))
                .addProtocol(new VERIFY_SUSPECT())
                .addProtocol(new BARRIER())
                .addProtocol(new NAKACK2())
                .addProtocol(new UNICAST3())
                .addProtocol(new STABLE())
                .addProtocol(new GMS())
                .addProtocol(new UFC())
                .addProtocol(new MFC())
                .addProtocol(new FRAG2())
                .addProtocol(new STATE());
        stack.init();
    }


    public boolean containsKey(String key) {
        synchronized (map){
            return map.containsKey(key);
        }
    }


    public Integer get(String key) {
        synchronized (map){
            return map.get(key);
        }
    }


    public void put(String key, Integer value) {
        try{
            channel.send(null, "put: " + "key: " + key + " value: " + value.toString());
        } catch(Exception e){
            e.printStackTrace();
        }
        map.put(key, value);
    }


    public Integer remove(String key) {
        try{
            channel.send(null, "remove: " + "key: " + key);
        } catch(Exception e){
            e.printStackTrace();
        }
        return map.remove(key);
    }

    //==================================================================

    @Override
    public void getState(OutputStream output) throws Exception{
        synchronized (map){
            Util.objectToStream(map, new DataOutputStream(output));
        }
    }

/////////////////////////////////// this part is used for partitioning
    @Override
    public void viewAccepted(View new_view){
        handleView(channel, new_view);
    }


    private static void handleView(JChannel ch, View new_view){
        if(new_view instanceof MergeView){
            ViewHandler handler = new ViewHandler(ch, (MergeView) new_view);
            // requires separate thread as we don't want to block JGroups
            handler.start();
        }
    }


    private static class ViewHandler extends Thread {
        JChannel ch;
        MergeView view;

        private ViewHandler(JChannel ch, MergeView view) {
            this.ch = ch;
            this.view = view;
        }

        public void run() {
            List<View> subgroups = view.getSubgroups();
            View tmp_view = subgroups.get(0);
            Address local_addr = ch.getAddress();
            if(!tmp_view.getMembers().contains(local_addr)) {
                System.out.println("Not member of the new primary partition ("
                        + tmp_view + "), will re-acquire the state");
                try {
                    ch.getState(null, 30000);
                } catch (Exception e) {

                }
            } else {
                System.out.println("Not member of the new primary partition ("
                        + tmp_view + "), will do nothing");
            }
        }
    }
//////////////////////////////////////////////////////////////////////////////////

    public void printState(){
        synchronized (map){
            for(Map.Entry<String,Integer> entry : map.entrySet()){
                System.out.println(entry);
            }
        }
    }


    @Override
    public void receive(Message msg){
        System.out.println(msg.getObject());
        if(msg.getSrc().equals(channel.getAddress())){
            return;
        }
        handleReceivedMessage((String) msg.getObject());
    }


    private void handleReceivedMessage(String message) {
        String operation = message.split(":")[0];
        String key = message.split("key: ")[1].split( " value: ")[0];
        synchronized (map){
            switch (operation){
                case "remove":
                    map.remove(key);
                    break;
                case "put":
                    String value = message.split("value: ")[1];
                    IntegerStringConverter isc = new IntegerStringConverter();
                    map.put(key, isc.fromString(value));
                    break;
                default:
                    break;
            }
        }
    }


    @Override
    public void setState(InputStream input) throws Exception{
        Map<String, Integer> tmpMap;
        tmpMap = (HashMap<String,Integer>) Util.objectFromStream(new DataInputStream(input));
        synchronized (map){
            map.clear();
            map.putAll(tmpMap);
        }
    }


    public void close() throws InterruptedException{
        Thread.sleep(20000);
        channel.close();
    }
}
