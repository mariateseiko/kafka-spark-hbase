package com.epam.bigdata.spark2;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class SparkStreamer {
    public static final String BID_ID = "bid_id";
    public static final String TSTAMP = "tstamp";
    public static final String IPINYOU_ID = "ipinyou_id";
    public static final String IP = "ip";
    public static final String REGION = "region";
    public static final String CITY = "city";
    public static final String AD_EXCHANGE = "ad_exchange";
    public static final String DOMAIN = "domain";
    public static final String URL = "url";
    public static final String ANON_URL_ID = "anon_url_id";
    public static final String SLOT_ID = "slot_id";
    public static final String SLOT_WIDTH = "slot_width";
    public static final String SLOT_HEIGHT = "slot_height";
    public static final String SLOT_VISIBILITY = "slot_visibility";
    public static final String SLOT_FORMAT = "slot_format";
    public static final String PAYING_PRICE = "paying_price";
    public static final String CREATIVE_ID = "creative_id";
    public static final String BIDDING_PRICE = "bidding_price";
    public static final String ADVERTISER_ID = "advertiser_id";
    public static final String USER_TAGS = "user_tags";
    public static final String STREAM_ID = "stream_id";
    public static final String DEVICE = "device";
    public static final String OS = "os";
    public static final String CHECKPOINT_DIR = "hdfs://quickstart.cloudera:8020/tmp/aux/spark/checkpoint";
    public static final String UNDEFINED = "UNDEFINED";
    public static final String NULL_STRING = "null";
    private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private static SimpleDateFormat sourceFormatter = new SimpleDateFormat("yyyyMMddhhmmss");

    public static void main(String[] args) throws IOException, InterruptedException{
        String quorum = args[0];
        String topic = args[1];
        String group = args[2];
        Integer threadsCount = Integer.parseInt(args[3]);
        String tableName = args[4];
        String columnFamily = args[5];


        SparkConf conf = new SparkConf().setAppName("Spark streaming");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(20));
        jsc.checkpoint(CHECKPOINT_DIR);

        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put(topic, threadsCount);

        Configuration hConf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");

        JavaPairReceiverInputDStream<String, String> logs = KafkaUtils.createStream(jsc, quorum, group, topicMap);
        logs.checkpoint(new Duration(1000));

        logs.foreachRDD(rdd -> rdd.foreach(kafkaInput -> {
                HTable logTable = new HTable(hConf, tableName);

                String[] fields = kafkaInput._2().split("\\t");

                UserAgent agent = UserAgent.parseUserAgentString(fields[3]);
                String os = agent.getOperatingSystem() != null ? agent.getOperatingSystem().getName() : UNDEFINED;
                String device =  os != null ? agent.getOperatingSystem().getDeviceType().getName()  : UNDEFINED;

            if (!fields[2].equals(NULL_STRING)) {
                String rowKey = fields[2] + "_" + fields[1];
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(BID_ID),
                        Bytes.toBytes(fields[0]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(TSTAMP),
                        Bytes.toBytes(formatter.format(sourceFormatter.parse(fields[1]))));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(IPINYOU_ID),
                        Bytes.toBytes(fields[2]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(IP),
                        Bytes.toBytes(fields[4]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(REGION),
                        Bytes.toBytes(Integer.parseInt(fields[5])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(CITY),
                        Bytes.toBytes(Integer.parseInt(fields[6])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(AD_EXCHANGE),
                        Bytes.toBytes(Integer.parseInt(fields[7])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(DOMAIN),
                        Bytes.toBytes(fields[8]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(URL),
                        Bytes.toBytes(fields[9]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(ANON_URL_ID),
                        Bytes.toBytes(fields[10]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(SLOT_ID), Bytes.toBytes(fields[11]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(SLOT_WIDTH), Bytes.toBytes(Integer.parseInt(fields[12])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(SLOT_HEIGHT), Bytes.toBytes(Integer.parseInt(fields[13])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(SLOT_VISIBILITY), Bytes.toBytes(Integer.parseInt(fields[14])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(SLOT_FORMAT), Bytes.toBytes(Integer.parseInt(fields[15])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(PAYING_PRICE), Bytes.toBytes(Integer.parseInt(fields[16])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(CREATIVE_ID), Bytes.toBytes(fields[17]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(BIDDING_PRICE), Bytes.toBytes(Integer.parseInt(fields[18])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(ADVERTISER_ID), Bytes.toBytes(Integer.parseInt(fields[19])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(USER_TAGS), Bytes.toBytes(Long.parseLong(fields[20])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(STREAM_ID), Bytes.toBytes(Integer.parseInt(fields[21])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(DEVICE), Bytes.toBytes(device));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(OS), Bytes.toBytes(os));

                try {
                    logTable.put(put);
                } catch (IOException e) {
                    System.err.println(e.getMessage());
                }
            }
            logTable.flushCommits();
            logTable.close();
        }));

        jsc.start();
        jsc.awaitTermination();
    }

}
