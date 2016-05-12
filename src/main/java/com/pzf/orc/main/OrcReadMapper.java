package com.pzf.orc.main;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrcReadMapper extends Mapper<NullWritable, OrcStruct, Text, IntWritable> {

	private StructObjectInspector oip;
	private final OrcSerde serde = new OrcSerde();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Properties table = new Properties();
		table.setProperty("columns", "ct,ip");
		table.setProperty("columns.types", "string,string,struct<ct:string,ip:string>");
		serde.initialize(context.getConfiguration(), table);
		try {
			oip = (StructObjectInspector) serde.getObjectInspector();
		} catch (SerDeException e) {
			e.printStackTrace();
		}
	}

	protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
		List<? extends StructField> fields = oip.getAllStructFieldRefs();
		StringObjectInspector bInspector = (StringObjectInspector) fields.get(1).getFieldObjectInspector();
		String b = "ip";
		try {
			b = bInspector.getPrimitiveJavaObject(oip.getStructFieldData(serde.deserialize(value), fields.get(1)));
		} catch (SerDeException e1) {
			e1.printStackTrace();
		}
		System.out.println(b);
		context.write(new Text(b), new IntWritable(1));
	}
}
