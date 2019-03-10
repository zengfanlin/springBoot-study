package com.pancm.storm;

import java.util.Iterator;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class SplitFunc extends BaseFunction {

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String str=tuple.getStringByField("str");
		String[] attrs=str.split("\\|");
		if(attrs.length>2) {
			Values values=new Values();
			for (String attr: attrs) {
				values.add(attr);
			}
			collector.emit(values);
		}
		
	}

}
