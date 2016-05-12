package com.pzf.orc.main;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class OrcReader {
	private final static String inputSchema = "struct<ct:string,ip:string>";
	private final static String outputSchema = "struct<c1:string,c2:int>";

	private static StructObjectInspector inputInspector;
	private static SettableStructObjectInspector outputInspector;
	private static OrcSerde serde;

	public static class OrcReaderMap extends Mapper<NullWritable, OrcStruct, NullWritable, Writable> {

		public void setup(Context context) throws IOException, InterruptedException {
			TypeInfo inputTypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(inputSchema);
			inputInspector = (StructObjectInspector) OrcStruct.createObjectInspector(inputTypeInfo);

			TypeInfo outputTypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(outputSchema);
			outputInspector = (SettableStructObjectInspector) OrcStruct.createObjectInspector(outputTypeInfo);

			serde = new OrcSerde();
			List<? extends StructField> fieldList = outputInspector.getAllStructFieldRefs();
			StringBuffer columnStr = new StringBuffer();
			StringBuffer typeStr = new StringBuffer();
			for (StructField sf : fieldList) {
				if (columnStr.length() > 0) {
					columnStr.append(",");
				}
				columnStr.append(sf.getFieldName());
				if (typeStr.length() > 0) {
					typeStr.append(",");
				}
				typeStr.append(sf.getFieldObjectInspector().getTypeName());
			}
			Properties props = new Properties();
			props.put(IOConstants.COLUMNS, columnStr.toString());
			props.put(IOConstants.COLUMNS_TYPES, typeStr.toString());
			serde.initialize(context.getConfiguration(), props);
		}

		public void map(NullWritable meaningless, OrcStruct value, Context context)
				throws IOException, InterruptedException {
			List<Object> list = inputInspector.getStructFieldsDataAsList(value);
			Text f1 = (Text) list.get(0);
			Text f2 = (Text) list.get(1);
			context.write(NullWritable.get(), f1);
			context.write(NullWritable.get(), f2);
			
//			OrcStruct objOut = (OrcStruct) outputInspector.create();
//			List<? extends StructField> flst = outputInspector.getAllStructFieldRefs();
//			outputInspector.setStructFieldData(objOut, flst.get(0), f1);
//			outputInspector.setStructFieldData(objOut, flst.get(1), f2);
//			context.write(NullWritable.get(), serde.serialize(objOut, outputInspector));
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		conf.set("mapreduce.input.fileinputformat.inputdir", "/tmp/orc/write_output");
		conf.set("mapreduce.output.fileoutputformat.outputdir","/tmp/orc/output");
		
		if( conf.get("fs.defaultFS").equals("hdfs://localhost:9000")){
			deleteOutputPath(conf);				
		}
		
		Job job = Job.getInstance(conf, "ORC");
		job.setJarByClass(OrcReader.class);
		job.setInputFormatClass(OrcNewInputFormat.class);
//		job.setOutputFormatClass(OrcNewOutputFormat.class);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
//		job.setOutputKeyClass(NullWritable.class);
//		job.setOutputValueClass(Text.class);
		job.setMapperClass(OrcReader.OrcReaderMap.class);
		job.setNumReduceTasks(0);
		System.out.println(job.waitForCompletion(true) ? 0 : 1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	private static void deleteOutputPath(Configuration conf) {
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			Path outputPath = new Path(conf.get("mapreduce.output.fileoutputformat.outputdir"));
			boolean isExist = fs.exists(outputPath);
			if (isExist) {
				fs.delete(outputPath, true);
			}
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}