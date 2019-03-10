package com.pancm.storm;

import java.util.UUID;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopologyTrident {
	private final Logger logger = LoggerFactory.getLogger(TopologyApp.class);

	@Test
	public void demo() throws InterruptedException {
		String brokerZkStr = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
		BrokerHosts hosts = new ZkHosts(brokerZkStr); // 通过zookeeper中的/brokers即可找到kafka的地址
		String topic = "flux";
		String zkRoot = "/" + topic;
		String id = UUID.randomUUID().toString();
		TridentKafkaConfig sc = new TridentKafkaConfig(hosts, topic);
		sc.scheme = new SchemeAsMultiScheme(new StringScheme());

		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(sc);
		TridentTopology topology = new TridentTopology();
		Stream stream = topology.newStream("kafka_spout", spout);

		stream = stream.each(stream.getOutputFields(), new SplitFunc(), new Fields("domain", "url", "title", "referrer",
				"sh", "sw", "cd", "lang", "userAgent", "systemName", "account"));
		stream = stream.each(stream.getOutputFields(), new PrintFilter());
		LocalCluster cluster = new LocalCluster();
		Config config = new Config();

//		本地提交
		cluster.submitTopology("Flux_Spout", config, topology.build());
		logger.info("storm启动成功...");
		Thread.sleep(1000 * 600);
		cluster.killTopology("");
		cluster.shutdown();
	}
}
