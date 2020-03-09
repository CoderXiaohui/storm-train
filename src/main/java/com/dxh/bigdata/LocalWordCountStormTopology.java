package com.dxh.bigdata;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * @Author: Dxh
 * @Date: 2020/3/9 23:36
 * @Description:
 */
public class LocalWordCountStormTopology {

    public static class DataSourceSpout extends BaseRichSpout{
        private SpoutOutputCollector spoutOutputCollector;

        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.spoutOutputCollector = spoutOutputCollector;
        }

        @Override
        public void nextTuple() {
            Collection<File>  files =  FileUtils.listFiles(new File("E:\\stormtest"),new String[]{"txt"},true);
            for (File file:files){
                try {
                    List<String> lines  =   FileUtils.readLines(file);
                    for (String words:lines){
                        spoutOutputCollector.emit(new Values(words));
                    }

                    //此方法一直循环(nextTuple)，所以要把处理完的文件改名
                    FileUtils.moveFile(file,new File(file.getAbsolutePath()+System.currentTimeMillis()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("lines"));
        }
    }


    public static class SplitBolt extends BaseRichBolt{

        private OutputCollector outputCollector;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.outputCollector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            String line = tuple.getStringByField("lines");
            String[] words = line.split(",");
            for (String word : words){
                outputCollector.emit(new Values(word));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }




    public static class CountBolt extends BaseRichBolt{

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        }

        private  Map<String,Integer> map = new HashMap<>();


        @Override
        public void execute(Tuple tuple) {
           String word =  tuple.getStringByField("word");
           Integer count = map.get(word);
           if (null==count){
               count=0;
           }
           count++;
           map.put(word,count);
            System.out.println("=====");
           Set<Map.Entry<String,Integer>>  entrySet = map.entrySet();
           for (Map.Entry<String,Integer> entry:entrySet){
               System.out.println(entry);
           }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }


    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("DataSourceSpout",new DataSourceSpout());
        topologyBuilder.setBolt("SplitBolt",new SplitBolt()).shuffleGrouping("DataSourceSpout");
        topologyBuilder.setBolt("CountBolt",new CountBolt()).shuffleGrouping("SplitBolt");

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("LocalWordCountStormTopology",new Config(),topologyBuilder.createTopology());
    }



}
