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
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;


/**
 * <p>This class implements a {@link MergePolicy} that tries
 * to merge segments into levels of exponentially
 * increasing size, where each level has fewer segments than
 * the value of the merge factor. Whenever extra segments
 * (beyond the merge factor upper bound) are encountered,
 * all segments within the level are merged. You can get or
 * set the merge factor using {@link #getMergeFactor()} and
 * {@link #setMergeFactor(int)} respectively.</p>
 *
 * <p>This class is abstract and requires a subclass to
 * define the {@link #size} method which specifies how a
 * segment's size is determined.  {@link LogDocMergePolicy}
 * is one subclass that measures size by document count in
 * the segment.  {@link LogByteSizeMergePolicy} is another
 * subclass that measures size as the total byte size of the
 * file(s) for the segment.</p>
 */

public abstract class LogMergePolicy extends MergePolicy {

  /** Defines the allowed range of log(size) for each
   *  level.  A level is computed by taking the max segment
   *  log size, minus LEVEL_LOG_SPAN, and finding all
   *  segments falling within that range. */
  public static final double LEVEL_LOG_SPAN = 0.75;

  /** Default merge factor, which is how many segments are
   *  merged at a time */
  public static final int DEFAULT_MERGE_FACTOR = 10;

  /** Default maximum segment size.  A segment of this size
   *  or larger will never be merged.  @see setMaxMergeDocs */
  public static final int DEFAULT_MAX_MERGE_DOCS = Integer.MAX_VALUE;

  /** Default noCFSRatio.  If a merge's size is {@code >= 10%} of
   *  the index, then we disable compound file for it.
   *  @see MergePolicy#setNoCFSRatio */
  public static final double DEFAULT_NO_CFS_RATIO = 0.1;

  /** How many segments to merge at a time. */
  protected int mergeFactor = DEFAULT_MERGE_FACTOR;

  /** Any segments whose size is smaller than this value
   *  will be rounded up to this value.  This ensures that
   *  tiny segments are aggressively merged. */
  protected long minMergeSize;

  /** If the size of a segment exceeds this value then it
   *  will never be merged. */
  protected long maxMergeSize;

  // Although the core MPs set it explicitly, we must default in case someone
  // out there wrote his own LMP ...
  /** If the size of a segment exceeds this value then it
   * will never be merged during {@link IndexWriter#forceMerge}. */
  protected long maxMergeSizeForForcedMerge = Long.MAX_VALUE;

  /** If a segment has more than this many documents then it
   *  will never be merged. */
  protected int maxMergeDocs = DEFAULT_MAX_MERGE_DOCS;

  /** If true, we pro-rate a segment's size by the
   *  percentage of non-deleted documents. */
  protected boolean calibrateSizeByDeletes = true;

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  public LogMergePolicy() {
    super(DEFAULT_NO_CFS_RATIO, MergePolicy.DEFAULT_MAX_CFS_SEGMENT_SIZE);
  }

  /** Returns true if {@code LMP} is enabled in {@link
   *  IndexWriter}'s {@code infoStream}. */
  protected boolean verbose(IndexWriter writer) {
    return writer != null && writer.infoStream.isEnabled("LMP");
  }

  /** Print a debug message to {@link IndexWriter}'s {@code
   *  infoStream}. */
  protected void message(String message, IndexWriter writer) {
    if (verbose(writer)) {
      writer.infoStream.message("LMP", message);
    }
  }

  /** <p>Returns the number of segments that are merged at
   * once and also controls the total number of segments
   * allowed to accumulate in the index.</p> */
  public int getMergeFactor() {
    return mergeFactor;
  }

  /** Determines how often segment indices are merged by
   * addDocument().  With smaller values, less RAM is used
   * while indexing, and searches are
   * faster, but indexing speed is slower.  With larger
   * values, more RAM is used during indexing, and while
   * searches is slower, indexing is
   * faster.  Thus larger values ({@code > 10}) are best for batch
   * index creation, and smaller values ({@code < 10}) for indices
   * that are interactively maintained. */
  public void setMergeFactor(int mergeFactor) {
    if (mergeFactor < 2)
      throw new IllegalArgumentException("mergeFactor cannot be less than 2");
    this.mergeFactor = mergeFactor;
  }

  /** Sets whether the segment size should be calibrated by
   *  the number of deletes when choosing segments for merge. */
  public void setCalibrateSizeByDeletes(boolean calibrateSizeByDeletes) {
    this.calibrateSizeByDeletes = calibrateSizeByDeletes;
  }

  /** Returns true if the segment size should be calibrated 
   *  by the number of deletes when choosing segments for merge. */
  public boolean getCalibrateSizeByDeletes() {
    return calibrateSizeByDeletes;
  }

  /** Return the number of documents in the provided {@link
   *  SegmentCommitInfo}, pro-rated by percentage of
   *  non-deleted documents if {@link
   *  #setCalibrateSizeByDeletes} is set. */
  protected long sizeDocs(SegmentCommitInfo info, IndexWriter writer) throws IOException {
    if (calibrateSizeByDeletes) {
      int delCount = writer.numDeletedDocs(info);
      assert delCount <= info.info.maxDoc();
      return (info.info.maxDoc() - (long)delCount);
    } else {
      return info.info.maxDoc();
    }
  }

  /** Return the byte size of the provided {@link
   *  SegmentCommitInfo}, pro-rated by percentage of
   *  non-deleted documents if {@link
   *  #setCalibrateSizeByDeletes} is set. */
  protected long sizeBytes(SegmentCommitInfo info, IndexWriter writer) throws IOException {
    if (calibrateSizeByDeletes) {
      return super.size(info, writer);
    }
    return info.sizeInBytes();
  }
  
  /** Returns true if the number of segments eligible for
   *  merging is less than or equal to the specified {@code
   *  maxNumSegments}. */
  protected boolean isMerged(SegmentInfos infos, int maxNumSegments, Map<SegmentCommitInfo,Boolean> segmentsToMerge, IndexWriter writer) throws IOException {
    final int numSegments = infos.size();
    int numToMerge = 0;
    SegmentCommitInfo mergeInfo = null;
    boolean segmentIsOriginal = false;
    for(int i=0;i<numSegments && numToMerge <= maxNumSegments;i++) {
      final SegmentCommitInfo info = infos.info(i);
      final Boolean isOriginal = segmentsToMerge.get(info);
      if (isOriginal != null) {
        segmentIsOriginal = isOriginal;
        numToMerge++;
        mergeInfo = info;
      }
    }

    return numToMerge <= maxNumSegments &&
      (numToMerge != 1 || !segmentIsOriginal || isMerged(infos, mergeInfo, writer));
  }

  /**
   * Returns the merges necessary to merge the index, taking the max merge
   * size or max merge docs into consideration. This method attempts to respect
   * the {@code maxNumSegments} parameter, however it might be, due to size
   * constraints, that more than that number of segments will remain in the
   * index. Also, this method does not guarantee that exactly {@code
   * maxNumSegments} will remain, but &lt;= that number.
   */
  private MergeSpecification findForcedMergesSizeLimit(
      SegmentInfos infos, int maxNumSegments, int last, IndexWriter writer) throws IOException {
    MergeSpecification spec = new MergeSpecification();
    final List<SegmentCommitInfo> segments = infos.asList();

    int start = last - 1;
    while (start >= 0) {
      SegmentCommitInfo info = infos.info(start);
      if (size(info, writer) > maxMergeSizeForForcedMerge || sizeDocs(info, writer) > maxMergeDocs) {
        if (verbose(writer)) {
          message("findForcedMergesSizeLimit: skip segment=" + info + ": size is > maxMergeSize (" + maxMergeSizeForForcedMerge + ") or sizeDocs is > maxMergeDocs (" + maxMergeDocs + ")", writer);
        }
        // need to skip that segment + add a merge for the 'right' segments,
        // unless there is only 1 which is merged.
        if (last - start - 1 > 1 || (start != last - 1 && !isMerged(infos, infos.info(start + 1), writer))) {
          // there is more than 1 segment to the right of
          // this one, or a mergeable single segment.
          spec.add(new OneMerge(segments.subList(start + 1, last)));
        }
        last = start;
      } else if (last - start == mergeFactor) {
        // mergeFactor eligible segments were found, add them as a merge.
        spec.add(new OneMerge(segments.subList(start, last)));
        last = start;
      }
      --start;
    }

    // Add any left-over segments, unless there is just 1
    // already fully merged
    if (last > 0 && (++start + 1 < last || !isMerged(infos, infos.info(start), writer))) {
      spec.add(new OneMerge(segments.subList(start, last)));
    }

    return spec.merges.size() == 0 ? null : spec;
  }
  
  /**
   * Returns the merges necessary to forceMerge the index. This method constraints
   * the returned merges only by the {@code maxNumSegments} parameter, and
   * guaranteed that exactly that number of segments will remain in the index.
   */
  private MergeSpecification findForcedMergesMaxNumSegments(SegmentInfos infos, int maxNumSegments, int last, IndexWriter writer) throws IOException {
    MergeSpecification spec = new MergeSpecification();
    final List<SegmentCommitInfo> segments = infos.asList();

    // First, enroll all "full" merges (size
    // mergeFactor) to potentially be run concurrently:
    while (last - maxNumSegments + 1 >= mergeFactor) {
      spec.add(new OneMerge(segments.subList(last - mergeFactor, last)));
      last -= mergeFactor;
    }

    // Only if there are no full merges pending do we
    // add a final partial (< mergeFactor segments) merge:
    if (0 == spec.merges.size()) {
      if (maxNumSegments == 1) {

        // Since we must merge down to 1 segment, the
        // choice is simple:
        if (last > 1 || !isMerged(infos, infos.info(0), writer)) {
          spec.add(new OneMerge(segments.subList(0, last)));
        }
      } else if (last > maxNumSegments) {

        // Take care to pick a partial merge that is
        // least cost, but does not make the index too
        // lopsided.  If we always just picked the
        // partial tail then we could produce a highly
        // lopsided index over time:

        // We must merge this many segments to leave
        // maxNumSegments in the index (from when
        // forceMerge was first kicked off):
        final int finalMergeSize = last - maxNumSegments + 1;

        // Consider all possible starting points:
        long bestSize = 0;
        int bestStart = 0;

        for(int i=0;i<last-finalMergeSize+1;i++) {
          long sumSize = 0;
          for(int j=0;j<finalMergeSize;j++) {
            sumSize += size(infos.info(j+i), writer);
          }
          if (i == 0 || (sumSize < 2*size(infos.info(i-1), writer) && sumSize < bestSize)) {
            bestStart = i;
            bestSize = sumSize;
          }
        }

        spec.add(new OneMerge(segments.subList(bestStart, bestStart + finalMergeSize)));
      }
    }
    return spec.merges.size() == 0 ? null : spec;
  }
  
  /** Returns the merges necessary to merge the index down
   *  to a specified number of segments.
   *  This respects the {@link #maxMergeSizeForForcedMerge} setting.
   *  By default, and assuming {@code maxNumSegments=1}, only
   *  one segment will be left in the index, where that segment
   *  has no deletions pending nor separate norms, and it is in
   *  compound file format if the current useCompoundFile
   *  setting is true.  This method returns multiple merges
   *  (mergeFactor at a time) so the {@link MergeScheduler}
   *  in use may make use of concurrency. */
  @Override
  public MergeSpecification findForcedMerges(SegmentInfos infos,
            int maxNumSegments, Map<SegmentCommitInfo,Boolean> segmentsToMerge, IndexWriter writer) throws IOException {

    assert maxNumSegments > 0;
    if (verbose(writer)) {
      message("findForcedMerges: maxNumSegs=" + maxNumSegments + " segsToMerge="+ segmentsToMerge, writer);
    }

    // If the segments are already merged (e.g. there's only 1 segment), or
    // there are <maxNumSegments:.
    if (isMerged(infos, maxNumSegments, segmentsToMerge, writer)) {
      if (verbose(writer)) {
        message("already merged; skip", writer);
      }
      return null;
    }

    // Find the newest (rightmost) segment that needs to
    // be merged (other segments may have been flushed
    // since merging started):
    int last = infos.size();
    while (last > 0) {
      final SegmentCommitInfo info = infos.info(--last);
      if (segmentsToMerge.get(info) != null) {
        last++;
        break;
      }
    }

    if (last == 0) {
      if (verbose(writer)) {
        message("last == 0; skip", writer);
      }
      return null;
    }
    
    // There is only one segment already, and it is merged
    if (maxNumSegments == 1 && last == 1 && isMerged(infos, infos.info(0), writer)) {
      if (verbose(writer)) {
        message("already 1 seg; skip", writer);
      }
      return null;
    }

    // Check if there are any segments above the threshold
    boolean anyTooLarge = false;
    for (int i = 0; i < last; i++) {
      SegmentCommitInfo info = infos.info(i);
      if (size(info, writer) > maxMergeSizeForForcedMerge || sizeDocs(info, writer) > maxMergeDocs) {
        anyTooLarge = true;
        break;
      }
    }

    if (anyTooLarge) {
      return findForcedMergesSizeLimit(infos, maxNumSegments, last, writer);
    } else {
      return findForcedMergesMaxNumSegments(infos, maxNumSegments, last, writer);
    }
  }

  /**
   * Finds merges necessary to force-merge all deletes from the
   * index.  We simply merge adjacent segments that have
   * deletes, up to mergeFactor at a time.
   */ 
  @Override
  public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, IndexWriter writer)
      throws IOException {
    final List<SegmentCommitInfo> segments = segmentInfos.asList();
    final int numSegments = segments.size();

    if (verbose(writer)) {
      message("findForcedDeleteMerges: " + numSegments + " segments", writer);
    }

    MergeSpecification spec = new MergeSpecification();
    int firstSegmentWithDeletions = -1;
    assert writer != null;
    for(int i=0;i<numSegments;i++) {
      final SegmentCommitInfo info = segmentInfos.info(i);
      int delCount = writer.numDeletedDocs(info);
      if (delCount > 0) {
        if (verbose(writer)) {
          message("  segment " + info.info.name + " has deletions", writer);
        }
        if (firstSegmentWithDeletions == -1)
          firstSegmentWithDeletions = i;
        else if (i - firstSegmentWithDeletions == mergeFactor) {
          // We've seen mergeFactor segments in a row with
          // deletions, so force a merge now:
          if (verbose(writer)) {
            message("  add merge " + firstSegmentWithDeletions + " to " + (i-1) + " inclusive", writer);
          }
          spec.add(new OneMerge(segments.subList(firstSegmentWithDeletions, i)));
          firstSegmentWithDeletions = i;
        }
      } else if (firstSegmentWithDeletions != -1) {
        // End of a sequence of segments with deletions, so,
        // merge those past segments even if it's fewer than
        // mergeFactor segments
        if (verbose(writer)) {
          message("  add merge " + firstSegmentWithDeletions + " to " + (i-1) + " inclusive", writer);
        }
        spec.add(new OneMerge(segments.subList(firstSegmentWithDeletions, i)));
        firstSegmentWithDeletions = -1;
      }
    }

    if (firstSegmentWithDeletions != -1) {
      if (verbose(writer)) {
        message("  add merge " + firstSegmentWithDeletions + " to " + (numSegments-1) + " inclusive", writer);
      }
      spec.add(new OneMerge(segments.subList(firstSegmentWithDeletions, numSegments)));
    }

    return spec;
  }

  private static class SegmentInfoAndLevel implements Comparable<SegmentInfoAndLevel> {
    SegmentCommitInfo info;
    float level;
    int index;
    
    public SegmentInfoAndLevel(SegmentCommitInfo info, float level, int index) {
      this.info = info;
      this.level = level;
      this.index = index;
    }

    // Sorts largest to smallest
    @Override
    public int compareTo(SegmentInfoAndLevel other) {
      return Float.compare(other.level, level);
    }
  }

  /** Checks if any merges are now necessary and returns a
   *  {@link MergeSpecification} if so.  A merge
   *  is necessary when there are more than {@link
   *  #setMergeFactor} segments at a given level.  When
   *  multiple levels have too many segments, this method
   *  will return multiple merges, allowing the {@link
   *  MergeScheduler} to use concurrency. */
  @Override
  public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos infos, IndexWriter writer) throws IOException {

    final int numSegments = infos.size();
    if (verbose(writer)) {
      message("findMerges: " + numSegments + " segments", writer);
    }

    // Compute levels, which is just log (base mergeFactor)
    // of the size of each segment
    final List<SegmentInfoAndLevel> levels = new ArrayList<>(numSegments);
    final float norm = (float) Math.log(mergeFactor);

    final Collection<SegmentCommitInfo> mergingSegments = writer.getMergingSegments();

    for(int i=0;i<numSegments;i++) {
      final SegmentCommitInfo info = infos.info(i);
      long size = size(info, writer);

      // Floor tiny segments
      if (size < 1) {
        size = 1;
      }

      final SegmentInfoAndLevel infoLevel = new SegmentInfoAndLevel(info, (float) Math.log(size)/norm, i);
      levels.add(infoLevel);

      if (verbose(writer)) {
        final long segBytes = sizeBytes(info, writer);
        String extra = mergingSegments.contains(info) ? " [merging]" : "";
        if (size >= maxMergeSize) {
          extra += " [skip: too large]";
        }
        message("seg=" + writer.segString(info) + " level=" + infoLevel.level + " size=" + String.format(Locale.ROOT, "%.3f MB", segBytes/1024/1024.) + extra, writer);
      }
    }

    final float levelFloor;
    if (minMergeSize <= 0)
      levelFloor = (float) 0.0;
    else
      levelFloor = (float) (Math.log(minMergeSize)/norm);

    // Now, we quantize the log values into levels.  The
    // first level is any segment whose log size is within
    // LEVEL_LOG_SPAN of the max size, or, who has such as
    // segment "to the right".  Then, we find the max of all
    // other segments and use that to define the next level
    // segment, etc.

    MergeSpecification spec = null;

    final int numMergeableSegments = levels.size();

    int start = 0;
    while(start < numMergeableSegments) {

      // Find max level of all segments not already
      // quantized.
      float maxLevel = levels.get(start).level;
      for(int i=1+start;i<numMergeableSegments;i++) {
        final float level = levels.get(i).level;
        if (level > maxLevel) {
          maxLevel = level;
        }
      }

      // Now search backwards for the rightmost segment that
      // falls into this level:
      float levelBottom;
      if (maxLevel <= levelFloor) {
        // All remaining segments fall into the min level
        levelBottom = -1.0F;
      } else {
        levelBottom = (float) (maxLevel - LEVEL_LOG_SPAN);

        // Force a boundary at the level floor
        if (levelBottom < levelFloor && maxLevel >= levelFloor) {
          levelBottom = levelFloor;
        }
      }

      int upto = numMergeableSegments-1;
      while(upto >= start) {
        if (levels.get(upto).level >= levelBottom) {
          break;
        }
        upto--;
      }
      if (verbose(writer)) {
        message("  level " + levelBottom + " to " + maxLevel + ": " + (1+upto-start) + " segments", writer);
      }

      // Finally, record all merges that are viable at this level:
      int end = start + mergeFactor;
      while(end <= 1+upto) {
        boolean anyTooLarge = false;
        boolean anyMerging = false;
        for(int i=start;i<end;i++) {
          final SegmentCommitInfo info = levels.get(i).info;
          anyTooLarge |= (size(info, writer) >= maxMergeSize || sizeDocs(info, writer) >= maxMergeDocs);
          if (mergingSegments.contains(info)) {
            anyMerging = true;
            break;
          }
        }

        if (anyMerging) {
          // skip
        } else if (!anyTooLarge) {
          if (spec == null)
            spec = new MergeSpecification();
          final List<SegmentCommitInfo> mergeInfos = new ArrayList<>(end-start);
          for(int i=start;i<end;i++) {
            mergeInfos.add(levels.get(i).info);
            assert infos.contains(levels.get(i).info);
          }
          if (verbose(writer)) {
            message("  add merge=" + writer.segString(mergeInfos) + " start=" + start + " end=" + end, writer);
          }
          spec.add(new OneMerge(mergeInfos));
        } else if (verbose(writer)) {
          message("    " + start + " to " + end + ": contains segment over maxMergeSize or maxMergeDocs; skipping", writer);
        }

        start = end;
        end = start + mergeFactor;
      }

      start = 1+upto;
    }

    return spec;
  }

  /** <p>Determines the largest segment (measured by
   * document count) that may be merged with other segments.
   * Small values (e.g., less than 10,000) are best for
   * interactive indexing, as this limits the length of
   * pauses while indexing to a few seconds.  Larger values
   * are best for batched indexing and speedier
   * searches.</p>
   *
   * <p>The default value is {@link Integer#MAX_VALUE}.</p>
   *
   * <p>The default merge policy ({@link
   * LogByteSizeMergePolicy}) also allows you to set this
   * limit by net size (in MB) of the segment, using {@link
   * LogByteSizeMergePolicy#setMaxMergeMB}.</p>
   */
  public void setMaxMergeDocs(int maxMergeDocs) {
    this.maxMergeDocs = maxMergeDocs;
  }

  /** Returns the largest segment (measured by document
   *  count) that may be merged with other segments.
   *  @see #setMaxMergeDocs */
  public int getMaxMergeDocs() {
    return maxMergeDocs;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("[" + getClass().getSimpleName() + ": ");
    sb.append("minMergeSize=").append(minMergeSize).append(", ");
    sb.append("mergeFactor=").append(mergeFactor).append(", ");
    sb.append("maxMergeSize=").append(maxMergeSize).append(", ");
    sb.append("maxMergeSizeForForcedMerge=").append(maxMergeSizeForForcedMerge).append(", ");
    sb.append("calibrateSizeByDeletes=").append(calibrateSizeByDeletes).append(", ");
    sb.append("maxMergeDocs=").append(maxMergeDocs).append(", ");
    sb.append("maxCFSSegmentSizeMB=").append(getMaxCFSSegmentSizeMB()).append(", ");
    sb.append("noCFSRatio=").append(noCFSRatio);
    sb.append("]");
    return sb.toString();
  }

}
