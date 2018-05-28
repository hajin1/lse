package lsModule;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class JmxCollector {
    String[] hosts;

    public JmxCollector(String allHost) {
        //parsing by comma
        hosts = allHost.split(",");
    }

    //collect JMX values by topic
    public long collectJmx(String topic) throws Exception {
        long val = 0;
        long total = 0;
        long diff = 0;
        String props = "kafka.server:type=BrokerTopicMetrics,name=Bytes*,topic=";
        for (String host : hosts) {
            String connectTo = "service:jmx:rmi:///jndi/rmi://" + host + ":9999/jmxrmi";
            String query = props + topic;

            //long t1 = System.nanoTime();
            val = getJmx(connectTo, query);
            //long t2 = System.nanoTime();
            //diff += (t2-t1);
            //System.out.println("host: "+host);
            //System.out.print("t1: "+t1+", t2: "+t2+", diff: "+(t2-t1));
            //System.out.println();
            total += val;
        }
//        System.out.println("total time: "+diff);
        return total;
    }

    //method Overloading
    //collect JMX values for all topics
    public long collectJmx() throws Exception {
        long val = 0;
        long total = 0;
//        String mbeanIn = "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec";
//        String mbeanOut = "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec";
        String mbean = "kafka.server:type=BrokerTopicMetrics,name=Bytes*";

        for (String host : hosts) {
            String connectTo = "service:jmx:rmi:///jndi/rmi://" + host + ":9999/jmxrmi";

//            System.out.println(System.nanoTime());
            long value = getJmx(connectTo, mbean);

            total += value;
        }
        return total;
    }

    private long getJmx(String connectTo, String pattern) throws Exception {
        String url = connectTo;
        JMXConnector connector;
        try {
            connector = JMXConnectorFactory.connect(new JMXServiceURL(url));
        } catch (NullPointerException e) {
            return error("Sorry, can't connect to: " + url);
        }

        MBeanServerConnection connection = connector.getMBeanServerConnection();
        String objectPattern = pattern != null ? pattern : "*:*";
        Set<ObjectName> objectNames = new TreeSet<ObjectName>(connection.queryNames(new ObjectName(objectPattern), null)); // metrics저장

        long val = 0;
        String tmp = "";
        int count = 0;
        long t1;
        long t2;
        long diff=0;
        for (ObjectName objectName : objectNames) {
            MBeanInfo mbeanInfo = connection.getMBeanInfo(objectName);// MBeans 값 get
            MBeanAttributeInfo[] attribute = mbeanInfo.getAttributes(); //불러온 MBeans의 attribute값 출력

            t1=System.nanoTime();
            diff = t1 - diff;

            Object object = connection.getAttribute(objectName, attribute[0].getName());
            tmp = object.toString();
            if (count < 1) {
                count++;
                val = Long.parseLong(tmp);
                System.out.println("InperSec count: "+count+", value: "+tmp);
            } else {
                val -= Long.parseLong(tmp);
                System.out.println("OutperSec count: "+count+", value: "+tmp);
                break;
            }
        }
        System.out.println("diff: "+diff);
        return val;
    }

    private int error(Object out) {
        System.out.println(out);
        return -1;
    }
}
