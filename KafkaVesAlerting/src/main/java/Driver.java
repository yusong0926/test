package main.java;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import evel_javalibrary.att.com.*;

import org.apache.log4j.Level;

import java.io.*;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;

import java.util.*;
import java.util.Properties;

import java.io.IOException;
import java.util.Map;


public class Driver
{
    private static Logger logger = Logger.getLogger(Driver.class);

    public static void main(String[] args) throws Exception {

        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream("config.properties");

            // load a properties file
            prop.load(input);

            // get the property value and print it out
            String event_api_url = prop.getProperty("event_api_url");
            int port = Integer.parseInt(prop.getProperty("port"));
            String path = prop.getProperty("path");
            String topic = prop.getProperty("topic");
            String username = prop.getProperty("username");
            String password = prop.getProperty("password");
            int totalCapacity = Integer.parseInt(prop.getProperty("total_capacity"));
            try {

                AgentMain.evel_initialize(event_api_url, port,
                        path, topic,
                        username,
                        password,
                        Level.DEBUG);
            } catch (Exception e) {
                e.printStackTrace();
            }

            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("group.id", "alert");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList("event-raw"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {
                    long startTime = System.nanoTime();
                    //System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                    String value = record.value();
                    int ind = value.indexOf(",");
                    String type = value.substring(0, ind);
                    String payload = value.substring(ind + 1, value.length());
                    Gson gson = new Gson();
                    Type stringStringMap = new TypeToken<Map<String, String>>() {
                    }.getType();

                    Map<String, String> map = gson.fromJson(payload, stringStringMap);
                    if (type.equals("enodeb")) {
                        if (alert(map, totalCapacity)) {
                            sendAlert(map);
                        }
                    }
                    long duration = System.nanoTime() - startTime;
                }


            }

        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public static boolean alert(Map<String, String> map, int totalCapacity) {
        if (Integer.valueOf(map.get("dlbitrate")) > totalCapacity * Integer.valueOf(map.get("dlallocrbrate"))/100.0) return true;
        if (Integer.valueOf(map.get("ulbitrate")) > totalCapacity * Integer.valueOf(map.get("ulallocrbrate"))/100.0) return true;
        return false;
    }

    public static void sendAlert(Map<String, String> map) throws Exception{
        logger.info("start sending alert");
        logger.debug("alerting events: " + map.toString());
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpPut httpPut = new HttpPut("http://10.128.13.3:8183/onos/progran/profile");
            httpPut.setEntity(new StringEntity("{\"Name\":\"video-slice\",\"DlSchedType\":\"RR\",\"UlSchedType\":\"RR\",\"DlAllocRBRate\":5,\"UlAllocRBRate\":20,\"CellIndividualOffset\":1,\"AdmControl\":2,\"MMECfg\":{\"IPAddr\":\"122.0.0.4\",\"Port\":36413},\"DlWifiRate\":0,\"Handover\":{\"A3Hysteresis\":1,\"A3TriggerQuantity\":0,\"A3offset\":0,\"A5Hysteresis\":1,\"A5Thresh1Rsrp\":80,\"A5Thresh1Rsrq\":20,\"A5Thresh2Rsrp\":90,\"A5Thresh2Rsrq\":20,\"A5TriggerQuantity\":0},\"Start\":\"\",\"End\":\"\",\"Status\":1}"));

            System.out.println("Executing request " + httpPut.getRequestLine());

            // Create a custom response handler
            ResponseHandler<String> responseHandler = response -> {
                int status = response.getStatusLine().getStatusCode();
                if (status >= 200 && status < 300) {
                    HttpEntity entity = response.getEntity();
                    return entity != null ? EntityUtils.toString(entity) : null;
                } else {
                    throw new ClientProtocolException("Unexpected response status: " + status);
                }
            };
            String responseBody = httpclient.execute(httpPut, responseHandler);
            System.out.println("----------------------------------------");
            System.out.println(responseBody);
        }

    }
}

