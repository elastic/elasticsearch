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
package org.apache.lucene5_shaded.index;


import org.apache.lucene5_shaded.codecs.PostingsFormat; // javadocs
import org.apache.lucene5_shaded.codecs.perfield.PerFieldPostingsFormat; // javadocs
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.IOContext;

/**
 * Holder class for common parameters used during read.
 * @lucene.experimental
 */
public class SegmentReadState {
  /** {@link Directory} where this segment is read from. */ 
  public final Directory directory;

  /** {@link SegmentInfo} describing this segment. */
  public final SegmentInfo segmentInfo;

  /** {@link FieldInfos} describing all fields in this
   *  segment. */
  public final FieldInfos fieldInfos;

  /** {@link IOContext} to pass to {@link
   *  Directory#openInput(String,IOContext)}. */
  public final IOContext context;

  /** Unique suffix for any postings files read for this
   *  segment.  {@link PerFieldPostingsFormat} sets this for
   *  each of the postings formats it wraps.  If you create
   *  a new {@link PostingsFormat} then any files you
   *  write/read must be derived using this suffix (use
   *  {@link IndexFileNames#segmentFileName(String,String,String)}). */
  public final String segmentSuffix;

  /** Create a {@code SegmentReadState}. */
  public SegmentReadState(Directory dir, SegmentInfo info,
      FieldInfos fieldInfos, IOContext context) {
    this(dir, info, fieldInfos,  context, "");
  }
  
  /** Create a {@code SegmentReadState}. */
  public SegmentReadState(Directory dir,
                          SegmentInfo info,
                          FieldInfos fieldInfos,
                          IOContext context,
                          String segmentSuffix) {
    this.directory = dir;
    this.segmentInfo = info;
    this.fieldInfos = fieldInfos;
    this.context = context;
    this.segmentSuffix = segmentSuffix;
  }

  /** Create a {@code SegmentReadState}. */
  public SegmentReadState(SegmentReadState other,
                          String newSegmentSuffix) {
    this.directory = other.directory;
    this.segmentInfo = other.segmentInfo;
    this.fieldInfos = other.fieldInfos;
    this.context = other.context;
    this.segmentSuffix = newSegmentSuffix;
  }
}
