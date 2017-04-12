package com.idvp.platform.hdfs;

public class HDFSWriterFactory {

	public static final String DataStreamType = "DataStream";

	public HDFSWriter getWriter(String fileType)  {
		if (fileType.equalsIgnoreCase(DataStreamType)) {
			return new HDFSDataStream();
		} else {
			throw new IllegalArgumentException("File type " + fileType + " not supported");
		}
	}
}
