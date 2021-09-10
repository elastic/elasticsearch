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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Set;

import org.apache.lucene5_shaded.store.Directory;

/** Embeds a [read-only] SegmentInfo and adds per-commit
 *  fields.
 *
 *  @lucene.experimental */
public class SegmentCommitInfo {
  
  /** The {@link SegmentInfo} that we wrap. */
  public final SegmentInfo info;

  // How many deleted docs in the segment:
  private int delCount;

  // Generation number of the live docs file (-1 if there
  // are no deletes yet):
  private long delGen;

  // Normally 1+delGen, unless an exception was hit on last
  // attempt to write:
  private long nextWriteDelGen;

  // Generation number of the FieldInfos (-1 if there are no updates)
  private long fieldInfosGen;
  
  // Normally 1+fieldInfosGen, unless an exception was hit on last attempt to
  // write
  private long nextWriteFieldInfosGen;
  
  // Generation number of the DocValues (-1 if there are no updates)
  private long docValuesGen;
  
  // Normally 1+dvGen, unless an exception was hit on last attempt to
  // write
  private long nextWriteDocValuesGen;

  // Track the per-field DocValues update files
  private final Map<Integer,Set<String>> dvUpdatesFiles = new HashMap<>();
  
  // TODO should we add .files() to FieldInfosFormat, like we have on
  // LiveDocsFormat?
  // track the fieldInfos update files
  private final Set<String> fieldInfosFiles = new HashSet<>();
  
  // Track the per-generation updates files
  @Deprecated
  private final Map<Long,Set<String>> genUpdatesFiles = new HashMap<>();
  
  private volatile long sizeInBytes = -1;

  /**
   * Sole constructor.
   * 
   * @param info
   *          {@link SegmentInfo} that we wrap
   * @param delCount
   *          number of deleted documents in this segment
   * @param delGen
   *          deletion generation number (used to name deletion files)
   * @param fieldInfosGen
   *          FieldInfos generation number (used to name field-infos files)
   * @param docValuesGen
   *          DocValues generation number (used to name doc-values updates files)
   */
  public SegmentCommitInfo(SegmentInfo info, int delCount, long delGen, long fieldInfosGen, long docValuesGen) {
    this.info = info;
    this.delCount = delCount;
    this.delGen = delGen;
    this.nextWriteDelGen = delGen == -1 ? 1 : delGen + 1;
    this.fieldInfosGen = fieldInfosGen;
    this.nextWriteFieldInfosGen = fieldInfosGen == -1 ? 1 : fieldInfosGen + 1;
    this.docValuesGen = docValuesGen;
    this.nextWriteDocValuesGen = docValuesGen == -1 ? 1 : docValuesGen + 1;
  }

  /**
   * Sets the updates file names per generation. Does not deep clone the map.
   * 
   * @deprecated required to support 4.6-4.8 indexes.
   */
  @Deprecated
  public void setGenUpdatesFiles(Map<Long,Set<String>> genUpdatesFiles) {
    this.genUpdatesFiles.clear();
    for (Entry<Long,Set<String>> kv : genUpdatesFiles.entrySet()) {
      // rename the set
      Set<String> set = new HashSet<>();
      for (String file : kv.getValue()) {
        set.add(info.namedForThisSegment(file));
      }
      this.genUpdatesFiles.put(kv.getKey(), set);
    }
  }
  
  /** Returns the per-field DocValues updates files. */
  public Map<Integer,Set<String>> getDocValuesUpdatesFiles() {
    return Collections.unmodifiableMap(dvUpdatesFiles);
  }
  
  /** Sets the DocValues updates file names, per field number. Does not deep clone the map. */
  public void setDocValuesUpdatesFiles(Map<Integer,Set<String>> dvUpdatesFiles) {
    this.dvUpdatesFiles.clear();
    for (Entry<Integer,Set<String>> kv : dvUpdatesFiles.entrySet()) {
      // rename the set
      Set<String> set = new HashSet<>();
      for (String file : kv.getValue()) {
        set.add(info.namedForThisSegment(file));
      }
      this.dvUpdatesFiles.put(kv.getKey(), set);
    }
  }
  
  /** Returns the FieldInfos file names. */
  public Set<String> getFieldInfosFiles() {
    return Collections.unmodifiableSet(fieldInfosFiles);
  }
  
  /** Sets the FieldInfos file names. */
  public void setFieldInfosFiles(Set<String> fieldInfosFiles) {
    this.fieldInfosFiles.clear();
    for (String file : fieldInfosFiles) {
      this.fieldInfosFiles.add(info.namedForThisSegment(file));
    }
  }

  /** Called when we succeed in writing deletes */
  void advanceDelGen() {
    delGen = nextWriteDelGen;
    nextWriteDelGen = delGen+1;
    sizeInBytes = -1;
  }

  /** Called if there was an exception while writing
   *  deletes, so that we don't try to write to the same
   *  file more than once. */
  void advanceNextWriteDelGen() {
    nextWriteDelGen++;
  }
  
  /** Gets the nextWriteDelGen. */
  long getNextWriteDelGen() {
    return nextWriteDelGen;
  }
  
  /** Sets the nextWriteDelGen. */
  void setNextWriteDelGen(long v) {
    nextWriteDelGen = v;
  }
  
  /** Called when we succeed in writing a new FieldInfos generation. */
  void advanceFieldInfosGen() {
    fieldInfosGen = nextWriteFieldInfosGen;
    nextWriteFieldInfosGen = fieldInfosGen + 1;
    sizeInBytes = -1;
  }
  
  /**
   * Called if there was an exception while writing a new generation of
   * FieldInfos, so that we don't try to write to the same file more than once.
   */
  void advanceNextWriteFieldInfosGen() {
    nextWriteFieldInfosGen++;
  }
  
  /** Gets the nextWriteFieldInfosGen. */
  long getNextWriteFieldInfosGen() {
    return nextWriteFieldInfosGen;
  }
  
  /** Sets the nextWriteFieldInfosGen. */
  void setNextWriteFieldInfosGen(long v) {
    nextWriteFieldInfosGen = v;
  }

  /** Called when we succeed in writing a new DocValues generation. */
  void advanceDocValuesGen() {
    docValuesGen = nextWriteDocValuesGen;
    nextWriteDocValuesGen = docValuesGen + 1;
    sizeInBytes = -1;
  }
  
  /**
   * Called if there was an exception while writing a new generation of
   * DocValues, so that we don't try to write to the same file more than once.
   */
  void advanceNextWriteDocValuesGen() {
    nextWriteDocValuesGen++;
  }

  /** Gets the nextWriteDocValuesGen. */
  long getNextWriteDocValuesGen() {
    return nextWriteDocValuesGen;
  }
  
  /** Sets the nextWriteDocValuesGen. */
  void setNextWriteDocValuesGen(long v) {
    nextWriteDocValuesGen = v;
  }
  
  /** Returns total size in bytes of all files for this
   *  segment. */
  public long sizeInBytes() throws IOException {
    if (sizeInBytes == -1) {
      long sum = 0;
      for (final String fileName : files()) {
        sum += info.dir.fileLength(fileName);
      }
      sizeInBytes = sum;
    }

    return sizeInBytes;
  }

  /** Returns all files in use by this segment. */
  public Collection<String> files() throws IOException {
    // Start from the wrapped info's files:
    Collection<String> files = new HashSet<>(info.files());

    // TODO we could rely on TrackingDir.getCreatedFiles() (like we do for
    // updates) and then maybe even be able to remove LiveDocsFormat.files().
    
    // Must separately add any live docs files:
    info.getCodec().liveDocsFormat().files(this, files);

    // Must separately add any per-gen updates files. This can go away when we
    // get rid of genUpdatesFiles (6.0)
    for (Set<String> updateFiles : genUpdatesFiles.values()) {
      files.addAll(updateFiles);
    }
    
    // must separately add any field updates files
    for (Set<String> updatefiles : dvUpdatesFiles.values()) {
      files.addAll(updatefiles);
    }
    
    // must separately add fieldInfos files
    files.addAll(fieldInfosFiles);
    
    return files;
  }

  // NOTE: only used in-RAM by IW to track buffered deletes;
  // this is never written to/read from the Directory
  private long bufferedDeletesGen;
  
  long getBufferedDeletesGen() {
    return bufferedDeletesGen;
  }

  void setBufferedDeletesGen(long v) {
    bufferedDeletesGen = v;
    sizeInBytes =  -1;
  }
  
  /** Returns true if there are any deletions for the 
   * segment at this commit. */
  public boolean hasDeletions() {
    return delGen != -1;
  }

  /** Returns true if there are any field updates for the segment in this commit. */
  public boolean hasFieldUpdates() {
    return fieldInfosGen != -1;
  }
  
  /** Returns the next available generation number of the FieldInfos files. */
  public long getNextFieldInfosGen() {
    return nextWriteFieldInfosGen;
  }
  
  /**
   * Returns the generation number of the field infos file or -1 if there are no
   * field updates yet.
   */
  public long getFieldInfosGen() {
    return fieldInfosGen;
  }
  
  /** Returns the next available generation number of the DocValues files. */
  public long getNextDocValuesGen() {
    return nextWriteDocValuesGen;
  }
  
  /**
   * Returns the generation number of the DocValues file or -1 if there are no
   * doc-values updates yet.
   */
  public long getDocValuesGen() {
    return docValuesGen;
  }
  
  /**
   * Returns the next available generation number
   * of the live docs file.
   */
  public long getNextDelGen() {
    return nextWriteDelGen;
  }

  /**
   * Returns generation number of the live docs file 
   * or -1 if there are no deletes yet.
   */
  public long getDelGen() {
    return delGen;
  }
  
  /**
   * Returns the number of deleted docs in the segment.
   */
  public int getDelCount() {
    return delCount;
  }

  void setDelCount(int delCount) {
    if (delCount < 0 || delCount > info.maxDoc()) {
      throw new IllegalArgumentException("invalid delCount=" + delCount + " (maxDoc=" + info.maxDoc() + ")");
    }
    this.delCount = delCount;
  }
  
  /** 
   * Returns a description of this segment. 
   * @deprecated Use {@link #toString(int)} instead.
   */
  @Deprecated
  public String toString(Directory dir, int pendingDelCount) {
    return toString(pendingDelCount);
  }

  /** Returns a description of this segment. */
  public String toString(int pendingDelCount) {
    String s = info.toString(delCount + pendingDelCount);
    if (delGen != -1) {
      s += ":delGen=" + delGen;
    }
    if (fieldInfosGen != -1) {
      s += ":fieldInfosGen=" + fieldInfosGen;
    }
    if (docValuesGen != -1) {
      s += ":dvGen=" + docValuesGen;
    }
    return s;
  }

  @Override
  public String toString() {
    return toString(0);
  }

  @Override
  public SegmentCommitInfo clone() {
    SegmentCommitInfo other = new SegmentCommitInfo(info, delCount, delGen, fieldInfosGen, docValuesGen);
    // Not clear that we need to carry over nextWriteDelGen
    // (i.e. do we ever clone after a failed write and
    // before the next successful write?), but just do it to
    // be safe:
    other.nextWriteDelGen = nextWriteDelGen;
    other.nextWriteFieldInfosGen = nextWriteFieldInfosGen;
    other.nextWriteDocValuesGen = nextWriteDocValuesGen;
    
    // deep clone
    for (Entry<Long,Set<String>> e : genUpdatesFiles.entrySet()) {
      other.genUpdatesFiles.put(e.getKey(), new HashSet<>(e.getValue()));
    }
    
    // deep clone
    for (Entry<Integer,Set<String>> e : dvUpdatesFiles.entrySet()) {
      other.dvUpdatesFiles.put(e.getKey(), new HashSet<>(e.getValue()));
    }
    
    other.fieldInfosFiles.addAll(fieldInfosFiles);
    
    return other;
  }
}
