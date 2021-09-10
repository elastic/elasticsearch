/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene5_shaded.codecs;


import java.io.IOException;

import org.apache.lucene5_shaded.index.SegmentInfo;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.IOContext;

/**
 * Expert: Controls the format of the 
 * {@link SegmentInfo} (segment metadata file).
 * @see SegmentInfo
 * @lucene.experimental
 */
public abstract class SegmentInfoFormat {
  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected SegmentInfoFormat() {
  }

  /**
   * Read {@link SegmentInfo} data from a directory.
   * @param directory directory to read from
   * @param segmentName name of the segment to read
   * @param segmentID expected identifier for the segment
   * @return infos instance to be populated with data
   * @throws IOException If an I/O error occurs
   */
  public abstract SegmentInfo read(Directory directory, String segmentName, byte segmentID[], IOContext context) throws IOException;

  /**
   * Write {@link SegmentInfo} data.
   * The codec must add its SegmentInfo filename(s) to {@code info} before doing i/o. 
   * @throws IOException If an I/O error occurs
   */
  public abstract void write(Directory dir, SegmentInfo info, IOContext ioContext) throws IOException;
}
