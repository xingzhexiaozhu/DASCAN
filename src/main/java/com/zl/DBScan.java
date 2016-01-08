package com.zl;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Mr.zhu on 2015/8/31.
 */
public class DBScan {

    //定义一个类:集合簇
    public class Cluster{
        private List<Tuple2<String, Long>> dataPoints = new ArrayList();//声明一个链表中存放集合中的点
        public List<Tuple2<String, Long>> getDataPoints(){ return dataPoints; } //获取一个集合中的各个点
        public void setDataPoints(List<Tuple2<String, Long>> dataPoints) { this.dataPoints = dataPoints;}
    }

    public void doDBScanAnalysis(List<Tuple2<String, Long>> dataPoints, long radius, int minPts){

       List<Cluster> clusterList = new ArrayList();//声明集合类簇对象

       for(int i=0; i<dataPoints.size(); i++){
           Tuple2<String, Long> dp =  dataPoints.get(i);
           List<Tuple2<String, Long>> arrivablePoints = isKeyAndReturnPoints(dp, dataPoints, radius, minPts);
           if(arrivablePoints != null){          //如果是核心对象
               Cluster tempCluster = new Cluster();
               tempCluster.setDataPoints(arrivablePoints);
               clusterList.add(tempCluster);     //增加一个类簇
           }
       }
       for(int i=0;i<clusterList.size();i++){    //对所有集合，将能合并的合并到一起
           for(int j=0;j<clusterList.size();j++){
               if(i!=j){
                   Cluster clusterA = clusterList.get(i);
                   Cluster clusterB = clusterList.get(j);

                   List<Tuple2<String, Long>> dpsA = clusterA.getDataPoints();
                   List<Tuple2<String, Long>> dpsB = clusterB.getDataPoints();

                   boolean flag=mergeList(dpsA,dpsB);
                   if(flag){                     //如果两个集合合并了，则设置当前集合j为合并后的集合
                       clusterList.set( j, new Cluster());
                   }
               }
           }
       }
       displayCluster(clusterList);
    }

    //输出聚类结果
    public void displayCluster(List<Cluster> clusterList){
        if(clusterList != null){
            //for(Cluster tempCluster:clusterList)
            int NotNullClusterCount = 0;
            for(int i=0; i<clusterList.size(); i++){
                if(clusterList.get(i).getDataPoints()!=null&&clusterList.get(i).getDataPoints().size()>0){
                    System.out.print("The " + (++NotNullClusterCount) + "st Cluster " + clusterList.get(i).getDataPoints().size() + ": ");
                    for(Tuple2<String, Long> dp:clusterList.get(i).getDataPoints()){
                        System.out.print("[" + dp._1() + "," + dp._2() + " ]; ");
                    }
                System.out.println("");
                }
            }
            System.out.println("Total Number of Clusters:"+ NotNullClusterCount);
        }
    }

    //判断当前元素是否为核心对象，如果是则返回以其为核心对象直接密度可达点的集合，不是则返回空
    private List<Tuple2<String, Long>> isKeyAndReturnPoints(Tuple2<String, Long> dataPoint, List<Tuple2<String, Long>> dataPoints, long radius, int minPts) {
        List<Tuple2<String, Long>> arrivablePoints = new ArrayList(); //用来存储所有直接密度可达对象

        for(Tuple2<String, Long> dp:dataPoints){//遍历链表中各个点，判断是否为当前对象的指定密度可达点
            double distance = getDistance(dataPoint,dp);
            if(distance <= radius){             //距离小于给定值则为直接密度可达点
                arrivablePoints.add(dp);
            }
        }
        if(arrivablePoints.size() >= minPts){//判断当前对象的直接密度可达点是否大于等于给定值
            return arrivablePoints;           //是就返回核心对象
        }
        return null;                           //不是就返回null
    }

    //返回两个对象之间的距离（本次应用以时间作为距离的度量）
    private long getDistance(Tuple2<String, Long> dp1, Tuple2<String, Long> dp2) {
        long num1 = dp1._2();
        long num2 = dp2._2();
        return Math.abs(num1 - num2);
    }

    //合并核心对象链表
    private boolean mergeList(List<Tuple2<String, Long>> dps1,List<Tuple2<String, Long>> dps2){
        boolean flag=false;
        if(dps1==null||dps2==null||dps1.size()==0||dps2.size()==0){
            return flag;
        }
        for(Tuple2<String, Long> dp:dps2){
            if(isContain(dp,dps1)){//判断集合2中的点是否与集合1中的点密度相连？（flag==true）:(flag==false)
                flag=true;
                break;
            }
        }
        if(flag){                  //两个集合有密度相连的点
            for(Tuple2<String, Long> dp:dps2){
                if(!isContain(dp,dps1)){ //逐一遍历集合2中的点，如果不在集合1中
                    dps1.add(dp);        //在就将合并到集合1中
                }
            }
        }
        return flag;
    }

    private boolean isContain(Tuple2<String, Long> dp,List<Tuple2<String, Long>> dps){
        boolean flag=false;
        for(Tuple2<String, Long> tempDp:dps){
            if(dp._1().equals(tempDp._1())){
                flag=true;
                break;
            }
        }
        return flag;
    }
}
