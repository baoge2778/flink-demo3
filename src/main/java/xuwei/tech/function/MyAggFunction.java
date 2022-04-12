package xuwei.tech.function;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;

/**
 * Created by xuwei.tech on 2018/11/14.
 */
public class MyAggFunction implements WindowFunction<Tuple3<Long, String, String>, Tuple4<String, String, String, Long>, Tuple, TimeWindow>{
    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<Long, String, String>> input, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
        //获取分组字段信息
        String sim = tuple.getField(0).toString();
        String lonlat = tuple.getField(1).toString();

        Iterator<Tuple3<Long, String, String>> it = input.iterator();

        //存储时间，为了获取最后一条数据的时间
        ArrayList<Long> arrayList = new ArrayList<>();

        long count = 0;
        while (it.hasNext()) {
            Tuple3<Long, String, String> next = it.next();
            arrayList.add(next.f0);
            count++;
        }

        System.err.println(Thread.currentThread().getId()+",window触发了，数据条数："+count);

        //排序
        Collections.sort(arrayList);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String time = sdf.format(new Date(arrayList.get(arrayList.size() - 1)));

        //组装结果
        Tuple4<String, String, String, Long> res = new Tuple4<>(time, sim, lonlat, count);

        out.collect(res);
    }


}