package com.dxh.bigdata;

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
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * 使用Storm实现累积求和的操作
 */
public class LocalSumStormTopology {

    /**
     * Spout需要继承BaseRiceSpout
     * 数据源需要产生数据并发射
     */
    public static class DataSourceSpout extends BaseRichSpout{


        private  SpoutOutputCollector spoutOutputCollector;


        /**
         * 初始化方法 ，是会被调用一次
         * @param map 配置参数
         * @param topologyContext 上下文
         * @param spoutOutputCollector
         */

        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.spoutOutputCollector = spoutOutputCollector;
        }

        int number = 0 ;

        /**
         * 会产生数据，在生产上肯定是从消息队列中获取数据
         * 死循环,一直不停的执行
         */
        @Override
        public void nextTuple() {
            this.spoutOutputCollector.emit(new Values(++number));

            System.out.println("Spout："+number);
            //防止数据产生太快
            Utils.sleep(1000);
        }

        /**
         * 声明输出字段
         * @param outputFieldsDeclarer
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            /**
             * 因为上面的this.spoutOutputCollector.emit(new Values(number++));  number是一个
             * 所以num也是一个
             * 如果是：
             * this.spoutOutputCollector.emit(new Values(number++,number2));
             * outputFieldsDeclarer.declare(new Fields("num","num2"));
             */
            outputFieldsDeclarer.declare(new Fields("num"));
        }
    }


    /**
     * 定义Bolt
     */
    public static class SumBolt extends BaseRichBolt {

        /**
         * 初始化方法
         * @param map 配置
         * @param topologyContext 上下文
         * @param outputCollector 往下传需要用到
         */
        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        }


        int sum = 0 ;
        /**
         * 也是死循环 ,职责：获取spout发送过来的数据，
         * @param tuple
         */
        @Override
        public void execute(Tuple tuple) {
            //Bolt中获取值，可以根据index获取，也可以根据上一个环节中定义的Field获取(建议使用Field)
            Integer value = tuple.getIntegerByField("num");
            sum +=value;

            System.out.println("[Bolt]Sum："+sum);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }

    /**
     *  Topology提交功能
     */
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout",new DataSourceSpout());
        builder.setBolt("SumBolt",new SumBolt()).shuffleGrouping("DataSourceSpout");

        /**
         * 创建一个本地的storm集群，本地模式不需要搭建Storm集群
         */
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalSumStormTopology",new Config(),builder.createTopology());
    }

}
