package com.pzf.orc.main;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OrcWriteReducer extends Reducer<Text, Text, Text, Text> {


	protected void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		
		Iterator<Text> itr = value.iterator();
		while (itr.hasNext()) {
			context.write(new Text(""), new Text(itr.next()));
		}
	}
}
