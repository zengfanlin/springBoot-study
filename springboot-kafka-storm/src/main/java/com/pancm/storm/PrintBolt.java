package com.pancm.storm;

import java.util.Map;

import org.apache.storm.generated.StreamInfo;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class PrintBolt extends BaseRichBolt{
	private OutputCollector collector;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector=collector;
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String key=input.getFields().get(0);
		String val=(String)input.getValueByField(key);
		System.out.println("key:"+key+"val:"+val);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
