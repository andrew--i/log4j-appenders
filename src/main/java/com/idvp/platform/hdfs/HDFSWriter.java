package com.idvp.platform.hdfs;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;

import java.io.IOException;

public interface HDFSWriter {
	public void open(String filePath) throws IOException;

	public void open(String filePath, CompressionCodec codec, SequenceFile.CompressionType cType) throws IOException;

	public void append(LoggingEvent e, Layout layout) throws IOException;

	public void sync() throws IOException;

	public void close() throws IOException;

	public boolean isUnderReplicated();
}
