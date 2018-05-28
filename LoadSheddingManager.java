package lsModule;

import com.sun.org.apache.xpath.internal.operations.Bool;
import sun.nio.cs.ext.JIS_X_0201;
import sun.plugin2.message.Message;

import javax.management.JMX;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class LoadSheddingManager {
    //public static Map<String, Boolean> jmxTopics = new HashMap<String, Boolean>();
    public static Map<String, Boolean> jmxTopics = Collections.synchronizedMap(new HashMap<String, Boolean>());
    //for all topics loadshedding
    boolean totalSwitch = false;


    private Map<String, String> conf = new HashMap<String, String>();

    public LoadSheddingManager(Map conf) {
        this.conf = conf;
    }

    // LoadShedding policy
    public long calculateThreshold() {
        long threshold = 10000000;
        return threshold;
    }

    public void initJmxTopics() {
        DbAdapter.getInstance(conf).initMethod(jmxTopics);
    }

    public static void main(String args[]) throws Exception {
        //configuration information
        Map<String, String> conf = new HashMap<String, String>();
        // for socket connection
        conf.put("hostname", "localhost");
        conf.put("port", "5004");
        // for DB connection
        conf.put("driverName", "org.mariadb.jdbc.Driver");
        conf.put("url", "jdbc:mariadb://localhost:3306/tutorial");
        conf.put("user", "root");
        conf.put("password", "1234");
        // for JMX
//        conf.put("hosts", "192.168.56.100,192.168.56.101,192.168.56.102");
        conf.put("hosts", "192.168.56.100");

        LoadSheddingManager lsm = new LoadSheddingManager(conf);

        lsm.initJmxTopics();
        lsm.start();
    }


    public void start() throws Exception {

        long threshold = 0;
        long currentJmx = 0;

        MessageReceiver messageReceiver = new MessageReceiver(jmxTopics, conf);
        JmxCollector collector = new JmxCollector(conf.get("hosts"));

        new Thread(messageReceiver).start();

        threshold = calculateThreshold();


        //loadshedding by topic
        while (true) {
            long topicThreshold = threshold / jmxTopics.size();
            Set<String> keySet = jmxTopics.keySet();

//            System.out.println("topicThreshold: " + topicThreshold);

            for (String key : keySet) {
                currentJmx = collector.collectJmx(key);
                String topic = key;
                System.out.print(topic + ": " + currentJmx + " ");

                if (currentJmx > topicThreshold && !jmxTopics.get(topic)) {
                    System.out.println("[" + topic + "][LOADSHEDDING ON!]");
                    DbAdapter.getInstance(conf).setSwicthValue(topic, "true");
                    jmxTopics.put(topic, true);
                }
                //currentJmx < threshod 인데 jmx switch가 true 이면 (로드쉐딩 on 이면)
                if (jmxTopics.get(topic) && currentJmx < topicThreshold) {
                    System.out.println("[" + topic + "][LOADSHEDDING OFF!]");
                    DbAdapter.getInstance(conf).setSwicthValue(topic, "false");
                    jmxTopics.put(topic, false);
                }

            }
            System.out.println();
            Thread.sleep(1000);
        }

        //loadshedding all topics
        /*long totalcurrentJmx;
        Set<String> keySet = jmxTopics.keySet();
        while (true) {
            totalcurrentJmx = collector.collectJmx();
            System.out.println("total: " + totalcurrentJmx);
            long tmp = 0;
            //currentJmx > threshold 인데 jmx switch가 false 이면 (로드쉐딩 off 이면)
            if (totalcurrentJmx > threshold && !totalSwitch) {
                System.out.println("[LOADSHEDDING ON!]");
                totalSwitch = true;
                for (String key : keySet) {
                    DbAdapter.getInstance(conf).setSwicthValue(key, "true");
                }
            }
            //currentJmx < threshod 인데 jmx switch가 true 이면 (로드쉐딩 on 이면)
            //DB에 접근해서 전체 토픽의 로드쉐딩 값을 false로 바꿔주는데 이럴 필요가 있겠지요?
            if (totalcurrentJmx < threshold && totalSwitch) {
                System.out.println("[LOADSHEDDING OFF!]");
                totalSwitch = false;
                //전체 다 바꾸는 함수 왜 안되는지?
//                DbAdapter.getInstance(conf).setSwicthValue("df");
                for (String key : keySet) {
                    DbAdapter.getInstance(conf).setSwicthValue(key, "false");
                }
            }
            Thread.sleep(1000);
        }*/

    }

}



