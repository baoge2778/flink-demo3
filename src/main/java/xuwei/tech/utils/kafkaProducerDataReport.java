package xuwei.tech.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 创建kafka topic的命令
 * bin/kafka-topics.sh  --create --topic simData --zookeeper localhost:2181 --partitions 5 --replication-factor 1
 *
 */
public class kafkaProducerDataReport {

    public static void main(String[] args) throws Exception{
        Properties prop = new Properties();
        //指定kafka broker地址
        prop.put("bootstrap.servers", "172.16.0.140:9092");
        //指定key value的序列化方式
        prop.put("key.serializer", StringSerializer.class.getName());
        prop.put("value.serializer", StringSerializer.class.getName());
        //指定topic名称
        String topic = "simData";

        //创建producer链接
        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(prop);

        //{"dt":"2018-01-01 10:11:22","sim":"14829569842","lonlat":"116.397128,39.916527"}

        //生产消息
//        while(true){
//            String message = "{\"dt\":\""+getCurrentTime()+"\",\"sim\":\""+getRandomSim()+"\",\"lonlat\":\""+getRandomLonLat(114,117, 36,39)+"\"}";
//            System.out.println(message);
//            producer.send(new ProducerRecord<String, String>(topic,message));
////            Thread.sleep(1000);
//        }
        int k=0;
        for (int i = 0; i < 1300000; i++) {
            String message = "{\"dt\":\""+getCurrentTime()+"\",\"sim\":\""+getRandomSim()+"\",\"lonlat\":\""+getRandomLonLat(114,117, 36,39)+"\"}";
            System.out.println(k++);
            producer.send(new ProducerRecord<String, String>(topic,message));
        }
        //关闭链接
        //producer.close();
    }

    public static String getCurrentTime(){
//        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
//        return sdf.format(new Date());
        return System.currentTimeMillis() + "";
    }

    public static String getRandomSim(){
        long sim = 14829500000L;
        Random random = new Random();
        int i = random.nextInt(99999);
        return sim+i+"";
    }


    /**
     * @Description: 在矩形内随机生成经纬度
     * @param MinLon：最小经度
     * 		  MaxLon： 最大经度
     *  	  MinLat：最小纬度
     * 		  MaxLat：最大纬度
     * @return @throws
     */
    public static String getRandomLonLat(double MinLon, double MaxLon, double MinLat, double MaxLat) {
//        BigDecimal db = new BigDecimal(Math.random() * (MaxLon - MinLon) + MinLon);
//        String lon = db.setScale(6, BigDecimal.ROUND_HALF_UP).toString();// 小数后6位
//        db = new BigDecimal(Math.random() * (MaxLat - MinLat) + MinLat);
//        String lat = db.setScale(6, BigDecimal.ROUND_HALF_UP).toString();
//        return lon + "," + lat;
         return "08001083803018A3C7CC3820B483A70F28839FCC3830F093A70F3801400048870150B6FD87FD0558A0CF3B60B9FD87FD0568007800A001029A020008001083803018A3C7CC3820B483A70F28839FCC3830F093A70F3801400048870150B6FD87FD0558A0CF3B60B9FD87FD0568007800A001029A020008001083803018A3C7CC3820B483A70F28839FCC3830F093A70F3801400048870150B6FD87FD0558A0CF3B60B9FD87FD0568007800A001029A020008001083803018A3C7CC3820B483A70F28839FCC3830F093A70F3801400048870150B6FD87FD0558A0CF3B60B9FD87FD0568007800A001029A020008001083803018A3C7CC3820B483A70F28839F00";
    }





}
