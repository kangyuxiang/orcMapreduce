package com.pzf.orc.util;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class SafeLineRecordReader extends RecordReader<LongWritable, Text> {
	
	private boolean initError = false;
	private LineRecordReader realReader = null;
	private static final Log LOG = LogFactory.getLog(SafeLineRecordReader.class);
	
	public SafeLineRecordReader(LineRecordReader realReader) {
		this.realReader = realReader;
	}

	public void initialize(InputSplit split, TaskAttemptContext context) 
	throws IOException, InterruptedException {
		try {
		    this.realReader.initialize(split, context);
		} catch (Exception e) {
			initError = true;
			LOG.error("line recordreader init error:" + ((FileSplit)split).getPath());
			Configuration conf2 = new Configuration();
		    FileSystem fs = FileSystem.get(conf2);
		    String fileRawPath = ((FileSplit)split).getPath().toString().replaceAll(":", "-").replaceAll("/", "_");
		    fs.mkdirs(new Path("/user/bd-warehouse/shc/badfile/"+fileRawPath));
		
		}
	}

	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (initError) return false;
		return this.realReader.nextKeyValue();
	}

	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return this.realReader.getCurrentKey();
	}

	public Text getCurrentValue() throws IOException, InterruptedException {
		return this.realReader.getCurrentValue();
	}

	public float getProgress() throws IOException, InterruptedException {
		if (initError) return 1.0f;
		return this.realReader.getProgress();
	}

	public void close() throws IOException {
		if (!initError) this.realReader.close();
	}

}

