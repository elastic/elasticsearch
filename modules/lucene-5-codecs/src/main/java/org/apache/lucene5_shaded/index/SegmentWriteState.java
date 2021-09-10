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
import org.apache.lucene5_shaded.util.InfoStream;
import org.apache.lucene5_shaded.util.MutableBits;

/**
 * Holder class for common parameters used during write.
 * @lucene.experimental
 */
public class SegmentWriteState {

  /** {@link InfoStream} used for debugging messages. */
  public final InfoStream infoStream;

  /** {@link Directory} where this segment will be written
   *  to. */
  public final Directory directory;

  /** {@link SegmentInfo} describing this segment. */
  public final SegmentInfo segmentInfo;

  /** {@link FieldInfos} describing all fields in this
   *  segment. */
  public final FieldInfos fieldInfos;

  /** Number of deleted documents set while flushing the
   *  segment. */
  public int delCountOnFlush;

  /**
   * Deletes and updates to apply while we are flushing the segment. A Term is
   * enrolled in here if it was deleted/updated at one point, and it's mapped to
   * the docIDUpto, meaning any docID &lt; docIDUpto containing this term should
   * be deleted/updated.
   */
  public final BufferedUpdates segUpdates;

  /** {@link MutableBits} recording live documents; this is
   *  only set if there is one or more deleted documents. */
  public MutableBits liveDocs;

  /** Unique suffix for any postings files written for this
   *  segment.  {@link PerFieldPostingsFormat} sets this for
   *  each of the postings formats it wraps.  If you create
   *  a new {@link PostingsFormat} then any files you
   *  write/read must be derived using this suffix (use
   *  {@link IndexFileNames#segmentFileName(String,String,String)}).
   *  
   *  Note: the suffix must be either empty, or be a textual suffix contain exactly two parts (separated by underscore), or be a base36 generation. */
  public final String segmentSuffix;
  
  /** {@link IOContext} for all writes; you should pass this
   *  to {@link Directory#createOutput(String,IOContext)}. */
  public final IOContext context;

  /** Sole constructor. */
  public SegmentWriteState(InfoStream infoStream, Directory directory, SegmentInfo segmentInfo, FieldInfos fieldInfos,
      BufferedUpdates segUpdates, IOContext context) {
    this(infoStream, directory, segmentInfo, fieldInfos, segUpdates, context, "");
  }

  /**
   * Constructor which takes segment suffix.
   * 
   * @see #SegmentWriteState(InfoStream, Directory, SegmentInfo, FieldInfos,
   *      BufferedUpdates, IOContext)
   */
  public SegmentWriteState(InfoStream infoStream, Directory directory, SegmentInfo segmentInfo, FieldInfos fieldInfos,
      BufferedUpdates segUpdates, IOContext context, String segmentSuffix) {
    this.infoStream = infoStream;
    this.segUpdates = segUpdates;
    this.directory = directory;
    this.segmentInfo = segmentInfo;
    this.fieldInfos = fieldInfos;
    assert assertSegmentSuffix(segmentSuffix);
    this.segmentSuffix = segmentSuffix;
    this.context = context;
  }
  
  /** Create a shallow copy of {@link SegmentWriteState} with a new segment suffix. */
  public SegmentWriteState(SegmentWriteState state, String segmentSuffix) {
    infoStream = state.infoStream;
    directory = state.directory;
    segmentInfo = state.segmentInfo;
    fieldInfos = state.fieldInfos;
    context = state.context;
    this.segmentSuffix = segmentSuffix;
    segUpdates = state.segUpdates;
    delCountOnFlush = state.delCountOnFlush;
    liveDocs = state.liveDocs;
  }
  
  // currently only used by assert? clean up and make real check?
  // either it's a segment suffix (_X_Y) or it's a parseable generation
  // TODO: this is very confusing how ReadersAndUpdates passes generations via
  // this mechanism, maybe add 'generation' explicitly to ctor create the 'actual suffix' here?
  private boolean assertSegmentSuffix(String segmentSuffix) {
    assert segmentSuffix != null;
    if (!segmentSuffix.isEmpty()) {
      int numParts = segmentSuffix.split("_").length;
      if (numParts == 2) {
        return true;
      } else if (numParts == 1) {
        Long.parseLong(segmentSuffix, Character.MAX_RADIX);
        return true;
      }
      return false; // invalid
    }
    return true;
  }
}
