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
package org.apache.lucene5_shaded.codecs.compressing;


import org.apache.lucene5_shaded.index.FieldInfo;
import org.apache.lucene5_shaded.index.MergeState;
import org.apache.lucene5_shaded.index.SegmentReader;

/** 
 * Computes which segments have identical field name to number mappings,
 * which allows stored fields and term vectors in this codec to be bulk-merged.
 */
class MatchingReaders {
  
  /** {@link SegmentReader}s that have identical field
   * name/number mapping, so their stored fields and term
   * vectors may be bulk merged. */
  final boolean[] matchingReaders;

  /** How many {@link #matchingReaders} are set. */
  final int count;
  
  MatchingReaders(MergeState mergeState) {
    // If the i'th reader is a SegmentReader and has
    // identical fieldName -> number mapping, then this
    // array will be non-null at position i:
    int numReaders = mergeState.maxDocs.length;
    int matchedCount = 0;
    matchingReaders = new boolean[numReaders];

    // If this reader is a SegmentReader, and all of its
    // field name -> number mappings match the "merged"
    // FieldInfos, then we can do a bulk copy of the
    // stored fields:

    nextReader:
    for (int i = 0; i < numReaders; i++) {
      for (FieldInfo fi : mergeState.fieldInfos[i]) {
        FieldInfo other = mergeState.mergeFieldInfos.fieldInfo(fi.number);
        if (other == null || !other.name.equals(fi.name)) {
          continue nextReader;
        }
      }
      matchingReaders[i] = true;
      matchedCount++;
    }
    
    this.count = matchedCount;

    if (mergeState.infoStream.isEnabled("SM")) {
      mergeState.infoStream.message("SM", "merge store matchedCount=" + count + " vs " + numReaders);
      if (count != numReaders) {
        mergeState.infoStream.message("SM", "" + (numReaders - count) + " non-bulk merges");
      }
    }
  }
}
