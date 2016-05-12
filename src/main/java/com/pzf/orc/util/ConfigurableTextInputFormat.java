package com.pzf.orc.util;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.hadoop.compression.lzo.LzoIndex;

public class ConfigurableTextInputFormat extends FileInputFormat {

	private static final Log LOG = LogFactory.getLog(ConfigurableTextInputFormat.class);

	private final Map<Path, LzoIndex> indexes = new HashMap<Path, LzoIndex>();

	public static boolean isLzoFile(String filename) {
		return filename.endsWith(".lzo");
	}

	/**
	 * Checks if the given filename ends in ".lzo.index".
	 * 
	 * @param filename
	 *            filename to check.
	 * @return true if the filename ends in ".lzo.index"
	 */
	public static boolean isLzoIndexFile(String filename) {
		return filename.endsWith(".lzo.index");
	}

	@Override
	protected List<FileStatus> listStatus(JobContext job) throws IOException {
		List<FileStatus> files = super.listStatus(job);

		Configuration conf = job.getConfiguration();

		for (Iterator<FileStatus> iterator = files.iterator(); iterator
				.hasNext();) {
			FileStatus fileStatus = iterator.next();
			Path file = fileStatus.getPath();
			FileSystem fs = file.getFileSystem(conf);
			if (!isLzoFile(file.toString())) {
				if (isLzoIndexFile(file.toString())) {
					iterator.remove();
				}
			} else {
				// read the index file
				LzoIndex index = LzoIndex.readIndex(fs, file);
				indexes.put(file, index);
			}
		}
		return files;
	}

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		if (isLzoFile(filename.toString())) {
			LzoIndex index = indexes.get(filename);
			return !index.isEmpty();
		} else {
			return super.isSplitable(context, filename);
		}
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> splits = super.getSplits(job);
		Configuration conf = job.getConfiguration();

		List<InputSplit> result = new ArrayList<InputSplit>();

		for (InputSplit genericSplit : splits) {
			FileSplit fileSplit = (FileSplit) genericSplit;
			Path file = fileSplit.getPath();
			FileSystem fs = file.getFileSystem(conf);

			if (!isLzoFile(file.toString())) {
				// non-LZO file, keep the input split as is.
				result.add(fileSplit);
				continue;
			}

			// LZO file, try to split if the .index file was found
			LzoIndex index = indexes.get(file);
			if (index == null) {
				throw new IOException("Index not found for " + file);
			}

			if (index.isEmpty()) {
				// empty index, keep as is
				result.add(fileSplit);
				continue;
			}

			long start = fileSplit.getStart();
			long end = start + fileSplit.getLength();

			long lzoStart = index.alignSliceStartToIndex(start, end);
			long lzoEnd = index.alignSliceEndToIndex(end, fs
					.getFileStatus(file).getLen());

			if (lzoStart != LzoIndex.NOT_FOUND && lzoEnd != LzoIndex.NOT_FOUND) {
				result.add(new FileSplit(file, lzoStart, lzoEnd - lzoStart,
						fileSplit.getLocations()));
			}
		}
		return result;
	}

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		FileSplit fileSplit = (FileSplit) split;
		if (fileSplit.getLength() == 0) {
			return new SafeLineRecordReader(new LineRecordReader());
		}
		if (fileSplit.getPath().toString().endsWith("lzo")) {
			return new LzoLineRecordReader();
		} else {
			return new SafeLineRecordReader(new LineRecordReader());

		}
	}

}
