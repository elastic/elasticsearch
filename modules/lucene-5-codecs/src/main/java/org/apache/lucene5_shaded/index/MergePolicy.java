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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.MergeInfo;
import org.apache.lucene5_shaded.store.RateLimiter;
import org.apache.lucene5_shaded.util.FixedBitSet;

/**
 * <p>Expert: a MergePolicy determines the sequence of
 * primitive merge operations.</p>
 * 
 * <p>Whenever the segments in an index have been altered by
 * {@link IndexWriter}, either the addition of a newly
 * flushed segment, addition of many segments from
 * addIndexes* calls, or a previous merge that may now need
 * to cascade, {@link IndexWriter} invokes {@link
 * #findMerges} to give the MergePolicy a chance to pick
 * merges that are now required.  This method returns a
 * {@link MergeSpecification} instance describing the set of
 * merges that should be done, or null if no merges are
 * necessary.  When IndexWriter.forceMerge is called, it calls
 * {@link #findForcedMerges(SegmentInfos,int,Map, IndexWriter)} and the MergePolicy should
 * then return the necessary merges.</p>
 *
 * <p>Note that the policy can return more than one merge at
 * a time.  In this case, if the writer is using {@link
 * SerialMergeScheduler}, the merges will be run
 * sequentially but if it is using {@link
 * ConcurrentMergeScheduler} they will be run concurrently.</p>
 * 
 * <p>The default MergePolicy is {@link
 * TieredMergePolicy}.</p>
 *
 * @lucene.experimental
 */
public abstract class MergePolicy {

  /** A map of doc IDs. */
  public static abstract class DocMap {
    /** Sole constructor, typically invoked from sub-classes constructors. */
    protected DocMap() {}

    /** Return the new doc ID according to its old value. */
    public abstract int map(int old);

    /** Useful from an assert. */
    boolean isConsistent(int maxDoc) {
      final FixedBitSet targets = new FixedBitSet(maxDoc);
      for (int i = 0; i < maxDoc; ++i) {
        final int target = map(i);
        if (target < 0 || target >= maxDoc) {
          assert false : "out of range: " + target + " not in [0-" + maxDoc + "[";
          return false;
        } else if (targets.get(target)) {
          assert false : target + " is already taken (" + i + ")";
          return false;
        }
      }
      return true;
    }
  }

  /** OneMerge provides the information necessary to perform
   *  an individual primitive merge operation, resulting in
   *  a single new segment.  The merge spec includes the
   *  subset of segments to be merged as well as whether the
   *  new segment should use the compound file format.
   *
   * @lucene.experimental */
  public static class OneMerge {

    SegmentCommitInfo info;         // used by IndexWriter
    boolean registerDone;           // used by IndexWriter
    long mergeGen;                  // used by IndexWriter
    boolean isExternal;             // used by IndexWriter
    int maxNumSegments = -1;        // used by IndexWriter

    /** Estimated size in bytes of the merged segment. */
    public volatile long estimatedMergeBytes;       // used by IndexWriter

    // Sum of sizeInBytes of all SegmentInfos; set by IW.mergeInit
    volatile long totalMergeBytes;

    List<SegmentReader> readers;        // used by IndexWriter

    /** Segments to be merged. */
    public final List<SegmentCommitInfo> segments;

    /** A private {@link RateLimiter} for this merge, used to rate limit writes and abort. */
    public final MergeRateLimiter rateLimiter;

    volatile long mergeStartNS = -1;

    /** Total number of documents in segments to be merged, not accounting for deletions. */
    public final int totalMaxDoc;
    Throwable error;

    /** Sole constructor.
     * @param segments List of {@link SegmentCommitInfo}s
     *        to be merged. */
    public OneMerge(List<SegmentCommitInfo> segments) {
      if (0 == segments.size()) {
        throw new RuntimeException("segments must include at least one segment");
      }
      // clone the list, as the in list may be based off original SegmentInfos and may be modified
      this.segments = new ArrayList<>(segments);
      int count = 0;
      for(SegmentCommitInfo info : segments) {
        count += info.info.maxDoc();
      }
      totalMaxDoc = count;

      rateLimiter = new MergeRateLimiter(this);
    }

    /** Called by {@link IndexWriter} after the merge is done and all readers have been closed. */
    public void mergeFinished() throws IOException {
    }

    /** Expert: Get the list of readers to merge. Note that this list does not
     *  necessarily match the list of segments to merge and should only be used
     *  to feed SegmentMerger to initialize a merge. When a {@link OneMerge}
     *  reorders doc IDs, it must override {@link #getDocMap} too so that
     *  deletes that happened during the merge can be applied to the newly
     *  merged segment. */
    public List<CodecReader> getMergeReaders() throws IOException {
      if (readers == null) {
        throw new IllegalStateException("IndexWriter has not initialized readers from the segment infos yet");
      }
      final List<CodecReader> readers = new ArrayList<>(this.readers.size());
      for (SegmentReader reader : this.readers) {
        if (reader.numDocs() > 0) {
          readers.add(reader);
        }
      }
      return Collections.unmodifiableList(readers);
    }
    
    /**
     * Expert: Sets the {@link SegmentCommitInfo} of the merged segment.
     * Allows sub-classes to e.g. set diagnostics properties.
     */
    public void setMergeInfo(SegmentCommitInfo info) {
      this.info = info;
    }

    /**
     * Returns the {@link SegmentCommitInfo} for the merged segment,
     * or null if it hasn't been set yet.
     */
    public SegmentCommitInfo getMergeInfo() {
      return info;
    }

    /** Expert: If {@link #getMergeReaders()} reorders document IDs, this method
     *  must be overridden to return a mapping from the <i>natural</i> doc ID
     *  (the doc ID that would result from a natural merge) to the actual doc
     *  ID. This mapping is used to apply deletions that happened during the
     *  merge to the new segment. */
    public DocMap getDocMap(MergeState mergeState) {
      return new DocMap() {
        @Override
        public int map(int docID) {
          return docID;
        }
      };
    }

    /** Record that an exception occurred while executing
     *  this merge */
    synchronized void setException(Throwable error) {
      this.error = error;
    }

    /** Retrieve previous exception set by {@link
     *  #setException}. */
    synchronized Throwable getException() {
      return error;
    }

    /** Returns a readable description of the current merge
     *  state. */
    public String segString() {
      StringBuilder b = new StringBuilder();
      final int numSegments = segments.size();
      for(int i=0;i<numSegments;i++) {
        if (i > 0) {
          b.append(' ');
        }
        b.append(segments.get(i).toString());
      }
      if (info != null) {
        b.append(" into ").append(info.info.name);
      }
      if (maxNumSegments != -1) {
        b.append(" [maxNumSegments=" + maxNumSegments + "]");
      }
      if (rateLimiter.getAbort()) {
        b.append(" [ABORTED]");
      }
      return b.toString();
    }
    
    /**
     * Returns the total size in bytes of this merge. Note that this does not
     * indicate the size of the merged segment, but the
     * input total size. This is only set once the merge is
     * initialized by IndexWriter.
     */
    public long totalBytesSize() throws IOException {
      return totalMergeBytes;
    }

    /**
     * Returns the total number of documents that are included with this merge.
     * Note that this does not indicate the number of documents after the merge.
     * */
    public int totalNumDocs() throws IOException {
      int total = 0;
      for (SegmentCommitInfo info : segments) {
        total += info.info.maxDoc();
      }
      return total;
    }

    /** Return {@link MergeInfo} describing this merge. */
    public MergeInfo getStoreMergeInfo() {
      return new MergeInfo(totalMaxDoc, estimatedMergeBytes, isExternal, maxNumSegments);
    }    
  }

  /**
   * A MergeSpecification instance provides the information
   * necessary to perform multiple merges.  It simply
   * contains a list of {@link OneMerge} instances.
   */

  public static class MergeSpecification {

    /**
     * The subset of segments to be included in the primitive merge.
     */

    public final List<OneMerge> merges = new ArrayList<>();

    /** Sole constructor.  Use {@link
     *  #add(OneMerge)} to add merges. */
    public MergeSpecification() {
    }

    /** Adds the provided {@link OneMerge} to this
     *  specification. */
    public void add(OneMerge merge) {
      merges.add(merge);
    }

    /** Returns a description of the merges in this
    *  specification. */
    public String segString(Directory dir) {
      StringBuilder b = new StringBuilder();
      b.append("MergeSpec:\n");
      final int count = merges.size();
      for(int i=0;i<count;i++) {
        b.append("  ").append(1 + i).append(": ").append(merges.get(i).segString());
      }
      return b.toString();
    }
  }

  /** Exception thrown if there are any problems while
   *  executing a merge. */
  public static class MergeException extends RuntimeException {
    private Directory dir;

    /** Create a {@code MergeException}. */
    public MergeException(String message, Directory dir) {
      super(message);
      this.dir = dir;
    }

    /** Create a {@code MergeException}. */
    public MergeException(Throwable exc, Directory dir) {
      super(exc);
      this.dir = dir;
    }

    /** Returns the {@link Directory} of the index that hit
     *  the exception. */
    public Directory getDirectory() {
      return dir;
    }
  }

  /** Thrown when a merge was explicity aborted because
   *  {@link IndexWriter#abortMerges} was called.  Normally
   *  this exception is privately caught and suppresed by
   *  {@link IndexWriter}. */
  public static class MergeAbortedException extends IOException {
    /** Create a {@link MergeAbortedException}. */
    public MergeAbortedException() {
      super("merge is aborted");
    }

    /** Create a {@link MergeAbortedException} with a
     *  specified message. */
    public MergeAbortedException(String message) {
      super(message);
    }
  }
  
  /**
   * Default ratio for compound file system usage. Set to <tt>1.0</tt>, always use 
   * compound file system.
   */
  protected static final double DEFAULT_NO_CFS_RATIO = 1.0;

  /**
   * Default max segment size in order to use compound file system. Set to {@link Long#MAX_VALUE}.
   */
  protected static final long DEFAULT_MAX_CFS_SEGMENT_SIZE = Long.MAX_VALUE;

  /** If the size of the merge segment exceeds this ratio of
   *  the total index size then it will remain in
   *  non-compound format */
  protected double noCFSRatio = DEFAULT_NO_CFS_RATIO;
  
  /** If the size of the merged segment exceeds
   *  this value then it will not use compound file format. */
  protected long maxCFSSegmentSize = DEFAULT_MAX_CFS_SEGMENT_SIZE;

  /**
   * Creates a new merge policy instance.
   */
  public MergePolicy() {
    this(DEFAULT_NO_CFS_RATIO, DEFAULT_MAX_CFS_SEGMENT_SIZE);
  }
  
  /**
   * Creates a new merge policy instance with default settings for noCFSRatio
   * and maxCFSSegmentSize. This ctor should be used by subclasses using different
   * defaults than the {@link MergePolicy}
   */
  protected MergePolicy(double defaultNoCFSRatio, long defaultMaxCFSSegmentSize) {
    this.noCFSRatio = defaultNoCFSRatio;
    this.maxCFSSegmentSize = defaultMaxCFSSegmentSize;
  }

  /**
   * Determine what set of merge operations are now necessary on the index.
   * {@link IndexWriter} calls this whenever there is a change to the segments.
   * This call is always synchronized on the {@link IndexWriter} instance so
   * only one thread at a time will call this method.
   * @param mergeTrigger the event that triggered the merge
   * @param segmentInfos
   *          the total set of segments in the index
   * @param writer the IndexWriter to find the merges on
   */
  public abstract MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, IndexWriter writer)
      throws IOException;

  /**
   * Determine what set of merge operations is necessary in
   * order to merge to {@code <=} the specified segment count. {@link IndexWriter} calls this when its
   * {@link IndexWriter#forceMerge} method is called. This call is always
   * synchronized on the {@link IndexWriter} instance so only one thread at a
   * time will call this method.
   * 
   * @param segmentInfos
   *          the total set of segments in the index
   * @param maxSegmentCount
   *          requested maximum number of segments in the index (currently this
   *          is always 1)
   * @param segmentsToMerge
   *          contains the specific SegmentInfo instances that must be merged
   *          away. This may be a subset of all
   *          SegmentInfos.  If the value is True for a
   *          given SegmentInfo, that means this segment was
   *          an original segment present in the
   *          to-be-merged index; else, it was a segment
   *          produced by a cascaded merge.
   * @param writer the IndexWriter to find the merges on
   */
  public abstract MergeSpecification findForcedMerges(
          SegmentInfos segmentInfos, int maxSegmentCount, Map<SegmentCommitInfo,Boolean> segmentsToMerge, IndexWriter writer)
      throws IOException;

  /**
   * Determine what set of merge operations is necessary in order to expunge all
   * deletes from the index.
   * 
   * @param segmentInfos
   *          the total set of segments in the index
   * @param writer the IndexWriter to find the merges on
   */
  public abstract MergeSpecification findForcedDeletesMerges(
      SegmentInfos segmentInfos, IndexWriter writer) throws IOException;

  /**
   * Returns true if a new segment (regardless of its origin) should use the
   * compound file format. The default implementation returns <code>true</code>
   * iff the size of the given mergedInfo is less or equal to
   * {@link #getMaxCFSSegmentSizeMB()} and the size is less or equal to the
   * TotalIndexSize * {@link #getNoCFSRatio()} otherwise <code>false</code>.
   */
  public boolean useCompoundFile(SegmentInfos infos, SegmentCommitInfo mergedInfo, IndexWriter writer) throws IOException {
    if (getNoCFSRatio() == 0.0) {
      return false;
    }
    long mergedInfoSize = size(mergedInfo, writer);
    if (mergedInfoSize > maxCFSSegmentSize) {
      return false;
    }
    if (getNoCFSRatio() >= 1.0) {
      return true;
    }
    long totalSize = 0;
    for (SegmentCommitInfo info : infos) {
      totalSize += size(info, writer);
    }
    return mergedInfoSize <= getNoCFSRatio() * totalSize;
  }
  
  /** Return the byte size of the provided {@link
   *  SegmentCommitInfo}, pro-rated by percentage of
   *  non-deleted documents is set. */
  protected long size(SegmentCommitInfo info, IndexWriter writer) throws IOException {
    long byteSize = info.sizeInBytes();
    int delCount = writer.numDeletedDocs(info);
    double delRatio = info.info.maxDoc() <= 0 ? 0.0f : (float) delCount / (float) info.info.maxDoc();
    assert delRatio <= 1.0;
    return (info.info.maxDoc() <= 0 ? byteSize : (long) (byteSize * (1.0 - delRatio)));
  }
  
  /** Returns true if this single info is already fully merged (has no
   *  pending deletes, is in the same dir as the
   *  writer, and matches the current compound file setting */
  protected final boolean isMerged(SegmentInfos infos, SegmentCommitInfo info, IndexWriter writer) throws IOException {
    assert writer != null;
    boolean hasDeletions = writer.numDeletedDocs(info) > 0;
    return !hasDeletions &&
      info.info.dir == writer.getDirectory() &&
      useCompoundFile(infos, info, writer) == info.info.getUseCompoundFile();
  }
  
  /** Returns current {@code noCFSRatio}.
   *
   *  @see #setNoCFSRatio */
  public double getNoCFSRatio() {
    return noCFSRatio;
  }

  /** If a merged segment will be more than this percentage
   *  of the total size of the index, leave the segment as
   *  non-compound file even if compound file is enabled.
   *  Set to 1.0 to always use CFS regardless of merge
   *  size. */
  public void setNoCFSRatio(double noCFSRatio) {
    if (noCFSRatio < 0.0 || noCFSRatio > 1.0) {
      throw new IllegalArgumentException("noCFSRatio must be 0.0 to 1.0 inclusive; got " + noCFSRatio);
    }
    this.noCFSRatio = noCFSRatio;
  }

  /** Returns the largest size allowed for a compound file segment */
  public final double getMaxCFSSegmentSizeMB() {
    return maxCFSSegmentSize/1024/1024.;
  }

  /** If a merged segment will be more than this value,
   *  leave the segment as
   *  non-compound file even if compound file is enabled.
   *  Set this to Double.POSITIVE_INFINITY (default) and noCFSRatio to 1.0
   *  to always use CFS regardless of merge size. */
  public void setMaxCFSSegmentSizeMB(double v) {
    if (v < 0.0) {
      throw new IllegalArgumentException("maxCFSSegmentSizeMB must be >=0 (got " + v + ")");
    }
    v *= 1024 * 1024;
    this.maxCFSSegmentSize = v > Long.MAX_VALUE ? Long.MAX_VALUE : (long) v;
  }
}
