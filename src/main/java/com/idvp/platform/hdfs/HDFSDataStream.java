/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.idvp.platform.hdfs;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HDFSDataStream extends AbstractHDFSWriter {

  private static final Logger logger = LoggerFactory.getLogger(HDFSDataStream.class);

  private FSDataOutputStream outStream;

  private EventSerializer serializer;
  private boolean useRawLocalFileSystem;


  @VisibleForTesting
  protected FileSystem getDfs(Configuration conf, Path dstPath) throws IOException {
    return dstPath.getFileSystem(conf);
  }

  protected void doOpen(Configuration conf, Path dstPath, FileSystem hdfs) throws IOException {
    if (useRawLocalFileSystem) {
      if (hdfs instanceof LocalFileSystem) {
        hdfs = ((LocalFileSystem)hdfs).getRaw();
      } else {
        logger.warn("useRawLocalFileSystem is set to true but file system " +
            "is not of type LocalFileSystem: " + hdfs.getClass().getName());
      }
    }

    boolean appending = false;
    if (conf.getBoolean("hdfs.append.support", false) == true && hdfs.isFile(dstPath)) {
      outStream = hdfs.append(dstPath);
      appending = true;
    } else {
      outStream = hdfs.create(dstPath);
    }

    serializer =  new BodyTextEventSerializer.Builder().build(outStream);
    if (appending && !serializer.supportsReopen()) {
      outStream.close();
      serializer = null;
      throw new IOException("serializer (" + "TEXT" +
          ") does not support append");
    }

    // must call superclass to check for replication issues
    registerCurrentStream(outStream, hdfs, dstPath);

    if (appending) {
      serializer.afterReopen();
    } else {
      serializer.afterCreate();
    }
  }

  @Override
  public void open(String filePath) throws IOException {
    Configuration conf = new Configuration();
    Path dstPath = new Path(filePath);
    FileSystem hdfs = getDfs(conf, dstPath);
    doOpen(conf, dstPath, hdfs);
  }

  @Override
  public void open(String filePath, CompressionCodec codec,
                   CompressionType cType) throws IOException {
    open(filePath);
  }

  @Override
  public void append(LoggingEvent e, Layout layout) throws IOException {
    serializer.write(e, layout);
  }

  @Override
  public void sync() throws IOException {
    serializer.flush();
    outStream.flush();
    hflushOrSync(outStream);
  }

  @Override
  public void close() throws IOException {
    serializer.flush();
    serializer.beforeClose();
    outStream.flush();
    hflushOrSync(outStream);
    outStream.close();

    unregisterCurrentStream();
  }

}
