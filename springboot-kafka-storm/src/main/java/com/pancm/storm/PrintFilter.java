package com.pancm.storm;

import java.util.Iterator;
import java.util.Map;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;

public class PrintFilter extends BaseFilter {
	public PrintFilter(String flag) {
		this.flag = flag;
	}

	public PrintFilter() {
		flag = "1";
	}

	private TridentOperationContext context = null;
	String flag = "";

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		this.context = context;
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		StringBuffer buffer = new StringBuffer("flag:" + flag + "总共有分区：" + context.getPartitionIndex());
		Fields fields = tuple.getFields();
		for (String field : fields) {
			Object value = tuple.getValueByField(field);
			buffer.append("属性：" + field + "=" + value);
		}
		System.out.println(buffer.toString());
		return true;
	}

}
