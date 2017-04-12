package com.idvp.platform.appender;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.idvp.platform.hdfs.BucketWriter;
import com.idvp.platform.hdfs.HDFSWriterFactory;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HDFSAppender extends AppenderSkeleton {

	private final int defaultCallTimeout = 10000;
	BucketWriter bucketWriter;

	private String filePath;
	private String fileName = "IDVPLogData";
	private long rollInterval = 10;
	private long rollSize = 1024*10;
	private long rollCount = 1024*10;
	private long batchSize = 100;
	private String inUsePrefix = "";
	private String inUseSuffix = ".tmp";
	private String fileSuffix = "";
	private CompressionCodec compressionCodec = null;
	private SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.BLOCK;
	private String writerType = HDFSWriterFactory.DataStreamType;
	private ExecutorService callTimeoutPool;
	private ScheduledExecutorService timedRollerPool;
	private int rollTimerPoolSize = 1;
	private int threadsPoolSize = 10;
	private int idleTimeout = 0;
	private long callTimeout = defaultCallTimeout;
	private long retryInterval = 180;
	private int maxCloseTires = Integer.MAX_VALUE;

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public void setRollTimerPoolSize(int rollTimerPoolSize) {
		this.rollTimerPoolSize = rollTimerPoolSize;
	}

	public void setThreadsPoolSize(int threadsPoolSize) {
		this.threadsPoolSize = threadsPoolSize;
	}

	public void setIdleTimeout(int idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public void setCallTimeout(long callTimeout) {
		this.callTimeout = callTimeout;
	}

	public void setRetryInterval(long retryInterval) {
		this.retryInterval = retryInterval;
	}

	public void setMaxCloseTires(int maxCloseTires) {
		this.maxCloseTires = maxCloseTires;
	}

	public void setRollInterval(long rollInterval) {
		this.rollInterval = rollInterval;
	}

	public void setRollSize(long rollSize) {
		this.rollSize = rollSize;
	}

	public void setRollCount(long rollCount) {
		this.rollCount = rollCount;
	}

	public void setBatchSize(long batchSize) {
		this.batchSize = batchSize;
	}

	public void setInUsePrefix(String inUsePrefix) {
		this.inUsePrefix = inUsePrefix;
	}

	public void setInUseSuffix(String inUseSuffix) {
		this.inUseSuffix = inUseSuffix;
	}

	public void setFileSuffix(String fileSuffix) {
		this.fileSuffix = fileSuffix;
	}

	public void setCompressionCodec(CompressionCodec compressionCodec) {
		this.compressionCodec = compressionCodec;
	}

	public void setCompressionType(SequenceFile.CompressionType compressionType) {
		this.compressionType = compressionType;
	}

	public void setWriterType(String writerType) {
		this.writerType = writerType;
	}

	@Override
	public void activateOptions() {

		String timeoutName = "hdfs-" + getName() + "-call-runner-%d";
		callTimeoutPool = Executors.newFixedThreadPool(threadsPoolSize,
				new ThreadFactoryBuilder().setNameFormat(timeoutName).build());

		String rollerName = "hdfs-" + getName() + "-roll-timer-%d";
		timedRollerPool = Executors.newScheduledThreadPool(rollTimerPoolSize,
				new ThreadFactoryBuilder().setNameFormat(rollerName).build());


		bucketWriter = new BucketWriter(rollInterval, rollSize, rollCount,
				batchSize, filePath, fileName, inUsePrefix, inUseSuffix,
				fileSuffix, compressionCodec, compressionType, new HDFSWriterFactory().getWriter(writerType),
				timedRollerPool, idleTimeout, callTimeout, callTimeoutPool, retryInterval, maxCloseTires);
	}

	protected void append(LoggingEvent loggingEvent) {
		try {
			bucketWriter.append(loggingEvent, layout);
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void close() {
		System.out.println("Closing");
		try {
			bucketWriter.close(true);
		} catch (IOException | InterruptedException e) {
			//ignore
		}

		// shut down all our thread pools
		ExecutorService[] toShutdown = {callTimeoutPool, timedRollerPool};
		for (ExecutorService execService : toShutdown) {
			execService.shutdown();
			try {
				while (!execService.isTerminated()) {
					execService.awaitTermination(
							Math.max(defaultCallTimeout, callTimeout), TimeUnit.MILLISECONDS);
				}
			} catch (InterruptedException ex) {
				//ignore
			}
		}

		callTimeoutPool = null;
		timedRollerPool = null;

	}

	public boolean requiresLayout() {
		return true;
	}
}
