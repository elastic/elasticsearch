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


import java.io.IOException;

/** This is a {@link LogMergePolicy} that measures size of a
 *  segment as the total byte size of the segment's files. */
public class LogByteSizeMergePolicy extends LogMergePolicy {

  /** Default minimum segment size.  @see setMinMergeMB */
  public static final double DEFAULT_MIN_MERGE_MB = 1.6;

  /** Default maximum segment size.  A segment of this size
   *  or larger will never be merged.  @see setMaxMergeMB */
  public static final double DEFAULT_MAX_MERGE_MB = 2048;

  /** Default maximum segment size.  A segment of this size
   *  or larger will never be merged during forceMerge.  @see setMaxMergeMBForForceMerge */
  public static final double DEFAULT_MAX_MERGE_MB_FOR_FORCED_MERGE = Long.MAX_VALUE;

  /** Sole constructor, setting all settings to their
   *  defaults. */
  public LogByteSizeMergePolicy() {
    minMergeSize = (long) (DEFAULT_MIN_MERGE_MB*1024*1024);
    maxMergeSize = (long) (DEFAULT_MAX_MERGE_MB*1024*1024);
    // NOTE: in Java, if you cast a too-large double to long, as we are doing here, then it becomes Long.MAX_VALUE
    maxMergeSizeForForcedMerge = (long) (DEFAULT_MAX_MERGE_MB_FOR_FORCED_MERGE*1024*1024);
  }
  
  @Override
  protected long size(SegmentCommitInfo info, IndexWriter writer) throws IOException {
    return sizeBytes(info, writer);
  }

  /** <p>Determines the largest segment (measured by total
   *  byte size of the segment's files, in MB) that may be
   *  merged with other segments.  Small values (e.g., less
   *  than 50 MB) are best for interactive indexing, as this
   *  limits the length of pauses while indexing to a few
   *  seconds.  Larger values are best for batched indexing
   *  and speedier searches.</p>
   *
   *  <p>Note that {@link #setMaxMergeDocs} is also
   *  used to check whether a segment is too large for
   *  merging (it's either or).</p>*/
  public void setMaxMergeMB(double mb) {
    maxMergeSize = (long) (mb*1024*1024);
  }

  /** Returns the largest segment (measured by total byte
   *  size of the segment's files, in MB) that may be merged
   *  with other segments.
   *  @see #setMaxMergeMB */
  public double getMaxMergeMB() {
    return ((double) maxMergeSize)/1024/1024;
  }

  /** <p>Determines the largest segment (measured by total
   *  byte size of the segment's files, in MB) that may be
   *  merged with other segments during forceMerge. Setting
   *  it low will leave the index with more than 1 segment,
   *  even if {@link IndexWriter#forceMerge} is called.*/
  public void setMaxMergeMBForForcedMerge(double mb) {
    maxMergeSizeForForcedMerge = (long) (mb*1024*1024);
  }

  /** Returns the largest segment (measured by total byte
   *  size of the segment's files, in MB) that may be merged
   *  with other segments during forceMerge.
   *  @see #setMaxMergeMBForForcedMerge */
  public double getMaxMergeMBForForcedMerge() {
    return ((double) maxMergeSizeForForcedMerge)/1024/1024;
  }

  /** Sets the minimum size for the lowest level segments.
   * Any segments below this size are considered to be on
   * the same level (even if they vary drastically in size)
   * and will be merged whenever there are mergeFactor of
   * them.  This effectively truncates the "long tail" of
   * small segments that would otherwise be created into a
   * single level.  If you set this too large, it could
   * greatly increase the merging cost during indexing (if
   * you flush many small segments). */
  public void setMinMergeMB(double mb) {
    minMergeSize = (long) (mb*1024*1024);
  }

  /** Get the minimum size for a segment to remain
   *  un-merged.
   *  @see #setMinMergeMB **/
  public double getMinMergeMB() {
    return ((double) minMergeSize)/1024/1024;
  }
}
