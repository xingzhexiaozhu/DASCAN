package com.zl;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

/**
 * Created by Mr.zhu on 2015/8/26.
 */
public class DataProccess {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("Data Proccess").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        if(args.length == 0){
            System.out.println("Must specify an access data file");
            System.exit(-1);
        }

        //用textFile来加载一个文件创建RDD,textFile的参数是一个path,这个path可以是：
        //1、 一个文件路径，这时候只装载指定的文件
        //2、 一个目录路径，这时候只装载指定目录下面的所有文件（不包括子目录下面的文件）
        //3、 通过通配符的形式加载多个文件或者加载多个目录下面的所有文件
        String dataFile = args[0];
        JavaRDD<String> dataLines = sc.textFile(dataFile);

        //过滤日志
        JavaRDD<String> filterData = dataLines.filter(Functions.DATA_FILTER).cache();
        //获取日志内容
        JavaRDD<DataFilter> accessData = filterData.map(Functions.PARSE_LOG_LINE).cache();

        // 获取在一个.txt中各个MAC地址上网次数
        List<Tuple2<String, Long>> MAC_total_Count = accessData
                .mapToPair(Functions.GET_MAC_COUNT)
                .reduceByKey(Functions.SUM_REDUCER)
                .take(5000);
        System.out.println("***************************************");
        System.out.println("MAC Count: " + MAC_total_Count);
        System.out.println("***************************************");

        //获取各个MAC地址及其对应的在一个.txt中总的上网时间
        List<Tuple2<String, Long>> MAC_total_Onlinetime = accessData
                .mapToPair(Functions.GET_MAC_ONLINETIME)
                .reduceByKey(Functions.SUM_REDUCER)
                .take(5000);
        System.out.println("***************************************");
        System.out.println("MAC OnlineTime: " + MAC_total_Onlinetime);
        System.out.println("***************************************");

        //引用密度聚类算法对上一步得到的结果进行分类（即根据上网时长划分的分类区间，得到人们上网时长一般会集中在哪些区间）
        //聚类算法：输入是上网时间（及MAC地址），输出是各上网时长区间及各区间对应的人数
        DBScan dbScan = new DBScan();
        dbScan.doDBScanAnalysis(MAC_total_Onlinetime, 3600, 3);//【数据:(MAC,onlineTime)，半径(聚类以上网时间划分)，范围(一个聚类中数据最少个数)】

        sc.stop();
    }
}
