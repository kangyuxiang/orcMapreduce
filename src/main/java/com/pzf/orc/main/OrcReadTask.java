package com.pzf.orc.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sohu.adrd.data.mapreduce.InputPathFilter;

public class OrcReadTask implements Tool {
	private Configuration conf = new Configuration();

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new OrcReadTask(), args);
		if (ret != 0) {
			System.err.println("Job Failed!");
			System.exit(ret);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		
		conf.set("fs.defaultFS","hdfs://localhost:9000");
		conf.set("io.compression.codecs", "com.hadoop.compression.lzo.LzopCodec");
		conf.set("io.compression.codec.lzo.class","com.hadoop.compression.lzo.LzoCodec");
		conf.set("mapreduce.input.fileinputformat.inputdir", "/tmp/orc/write_output");
		conf.set("mapreduce.output.fileoutputformat.outputdir","/tmp/orc/output");

		if( conf.get("fs.defaultFS").equals("hdfs://localhost:9000")){
			deleteOutputPath();				
		}

		Job job = Job.getInstance(conf, "ConsumerTask");

		job.setJarByClass(this.getClass());

		FileInputFormat.setInputPathFilter(job, InputPathFilter.class);
		job.setInputFormatClass(OrcNewInputFormat.class);

		job.setMapperClass(OrcReadMapper.class);
		job.setNumReduceTasks(0);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	
	private void deleteOutputPath() {
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

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;

	}
}
