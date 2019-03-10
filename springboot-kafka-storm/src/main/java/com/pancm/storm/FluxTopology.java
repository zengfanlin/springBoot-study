package com.pancm.storm;

import java.util.UUID;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringMultiSchemeWithTopic;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.pancm.constant.Constants;
import com.pancm.storm.bolt.InsertBolt;
import com.pancm.storm.spout.KafkaInsertDataSpout;

import kafka.api.OffsetRequest;


public class FluxTopology {
	private final Logger logger = LoggerFactory.getLogger(TopologyApp.class);
	@Test
	public void runStorm() throws InterruptedException {
		String brokerZkStr = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
		BrokerHosts hosts = new ZkHosts(brokerZkStr); // 通过zookeeper中的/brokers即可找到kafka的地址
		String topic = "flux";
		String zkRoot = "/" + topic;
		String id = UUID.randomUUID().toString();
		SpoutConfig sc = new SpoutConfig(hosts, topic, zkRoot, id);
		sc.scheme=new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout spout = new KafkaSpout(sc);
		
		PrintBolt printBolt = new PrintBolt();
		// 定义一个拓扑
		TopologyBuilder builder = new TopologyBuilder();
		// 设置1个Executeor(线程)，默认一个
		builder.setSpout("Flux_Spout", spout);
		// shuffleGrouping:表示是随机分组
		// 设置1个Executeor(线程)，和两个task
		builder.setBolt("Print_Bolt", printBolt).shuffleGrouping("Flux_Spout");

		StormTopology topology = builder.createTopology();
		LocalCluster cluster = new LocalCluster();
		Config config = new Config();
		// 设置一个应答者
		// conf.setNumAckers(1);
		// 设置一个work
		// conf.setNumWorkers(1);
//		没有参数时，本地提交
		cluster.submitTopology("Flux_Spout", config, topology);
		logger.info("storm启动成功...");
		Thread.sleep(1000*600);
		cluster.killTopology("");
		cluster.shutdown();
	}

	/**
	 * BrokerHosts hosts kafka集群列表 String topic 要消费的topic主题 String zkRoot
	 * kafka在zk中的目录（会在该节点目录下记录读取kafka消息的偏移量） String id 当前操作的标识id
	 */
	private  KafkaSpout createKafkaSpout() {
		String brokerZkStr = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
		BrokerHosts hosts = new ZkHosts(brokerZkStr); // 通过zookeeper中的/brokers即可找到kafka的地址
		String topic = "flux";
		String zkRoot = "/" + topic;
		String id = UUID.randomUUID().toString();
		SpoutConfig spoutConf = new SpoutConfig(hosts, topic, zkRoot, id);
		spoutConf.scheme=new SchemeAsMultiScheme(new StringScheme());
		// 本地环境设置之后，也可以在zk中建立/f-k-s节点，在集群环境中，不用配置也可以在zk中建立/f-k-s节点
		// spoutConf.zkServers = Arrays.asList(new String[]{"uplooking01",
		// "uplooking02", "uplooking03"});
		// spoutConf.zkPort = 2181;
//		spoutConf.startOffsetTime = OffsetRequest.LatestTime(); // 设置之后，刚启动时就不会把之前的消费也进行读取，会从最新的偏移量开始读取
		return new KafkaSpout(spoutConf);
	}
}
