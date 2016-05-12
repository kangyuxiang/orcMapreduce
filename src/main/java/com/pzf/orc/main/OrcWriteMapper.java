package com.pzf.orc.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class OrcWriteMapper extends Mapper<LongWritable, Text, NullWritable, Writable> {

	private final OrcSerde serde = new OrcSerde();
	private final String typeString = "struct<ct:string,ip:string>";
	private final TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeString);
	private final ObjectInspector inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String val[] = value.toString().split("\t");
		List<String> list = new ArrayList<String>();
		list.add(val[0]);
		list.add(val[1]);
		Writable row = serde.serialize(list, inspector);
		context.write(NullWritable.get(), row);
	}
}
