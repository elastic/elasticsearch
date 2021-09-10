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


/**
 * MergeTrigger is passed to
 * {@link MergePolicy#findMerges(MergeTrigger, SegmentInfos, IndexWriter)} to indicate the
 * event that triggered the merge.
 */
public enum MergeTrigger {
  /**
   * Merge was triggered by a segment flush.
   */
  SEGMENT_FLUSH,

  /**
   * Merge was triggered by a full flush. Full flushes
   * can be caused by a commit, NRT reader reopen or a close call on the index writer.
   */
  FULL_FLUSH,

  /**
   * Merge has been triggered explicitly by the user.
   */
  EXPLICIT,

  /**
   * Merge was triggered by a successfully finished merge.
   */
  MERGE_FINISHED,

  /**
   * Merge was triggered by a closing IndexWriter.
   */
  CLOSING
}
