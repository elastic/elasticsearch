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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene5_shaded.codecs.Codec;
import org.apache.lucene5_shaded.codecs.DocValuesConsumer;
import org.apache.lucene5_shaded.codecs.DocValuesFormat;
import org.apache.lucene5_shaded.codecs.FieldInfosFormat;
import org.apache.lucene5_shaded.codecs.LiveDocsFormat;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.store.FlushInfo;
import org.apache.lucene5_shaded.store.IOContext;
import org.apache.lucene5_shaded.store.TrackingDirectoryWrapper;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.IOUtils;
import org.apache.lucene5_shaded.util.MutableBits;

// Used by IndexWriter to hold open SegmentReaders (for
// searching or merging), plus pending deletes and updates,
// for a given segment
class ReadersAndUpdates {
  // Not final because we replace (clone) when we need to
  // change it and it's been shared:
  public final SegmentCommitInfo info;

  // Tracks how many consumers are using this instance:
  private final AtomicInteger refCount = new AtomicInteger(1);

  private final IndexWriter writer;

  // Set once (null, and then maybe set, and never set again):
  private SegmentReader reader;

  // Holds the current shared (readable and writable)
  // liveDocs.  This is null when there are no deleted
  // docs, and it's copy-on-write (cloned whenever we need
  // to change it but it's been shared to an external NRT
  // reader).
  private Bits liveDocs;

  // How many further deletions we've done against
  // liveDocs vs when we loaded it or last wrote it:
  private int pendingDeleteCount;

  // True if the current liveDocs is referenced by an
  // external NRT reader:
  private boolean liveDocsShared;

  // Indicates whether this segment is currently being merged. While a segment
  // is merging, all field updates are also registered in the
  // mergingNumericUpdates map. Also, calls to writeFieldUpdates merge the 
  // updates with mergingNumericUpdates.
  // That way, when the segment is done merging, IndexWriter can apply the
  // updates on the merged segment too.
  private boolean isMerging = false;
  
  private final Map<String,DocValuesFieldUpdates> mergingDVUpdates = new HashMap<>();
  
  public ReadersAndUpdates(IndexWriter writer, SegmentCommitInfo info) {
    this.writer = writer;
    this.info = info;
    liveDocsShared = true;
  }

  /** Init from a previously opened SegmentReader.
   *
   * <p>NOTE: steals incoming ref from reader. */
  public ReadersAndUpdates(IndexWriter writer, SegmentReader reader) {
    this.writer = writer;
    this.reader = reader;
    info = reader.getSegmentInfo();
    liveDocs = reader.getLiveDocs();
    liveDocsShared = true;
    pendingDeleteCount = reader.numDeletedDocs() - info.getDelCount();
    assert pendingDeleteCount >= 0: "got " + pendingDeleteCount + " reader.numDeletedDocs()=" + reader.numDeletedDocs() + " info.getDelCount()=" + info.getDelCount() + " maxDoc=" + reader.maxDoc() + " numDocs=" + reader.numDocs();
  }

  public void incRef() {
    final int rc = refCount.incrementAndGet();
    assert rc > 1;
  }

  public void decRef() {
    final int rc = refCount.decrementAndGet();
    assert rc >= 0;
  }

  public int refCount() {
    final int rc = refCount.get();
    assert rc >= 0;
    return rc;
  }

  public synchronized int getPendingDeleteCount() {
    return pendingDeleteCount;
  }
  
  // Call only from assert!
  public synchronized boolean verifyDocCounts() {
    int count;
    if (liveDocs != null) {
      count = 0;
      for(int docID=0;docID<info.info.maxDoc();docID++) {
        if (liveDocs.get(docID)) {
          count++;
        }
      }
    } else {
      count = info.info.maxDoc();
    }

    assert info.info.maxDoc() - info.getDelCount() - pendingDeleteCount == count: "info.maxDoc=" + info.info.maxDoc() + " info.getDelCount()=" + info.getDelCount() + " pendingDeleteCount=" + pendingDeleteCount + " count=" + count;
    return true;
  }

  /** Returns a {@link SegmentReader}. */
  public SegmentReader getReader(IOContext context) throws IOException {
    if (reader == null) {
      // We steal returned ref:
      reader = new SegmentReader(info, context);
      if (liveDocs == null) {
        liveDocs = reader.getLiveDocs();
      }
    }
    
    // Ref for caller
    reader.incRef();
    return reader;
  }
  
  public synchronized void release(SegmentReader sr) throws IOException {
    assert info == sr.getSegmentInfo();
    sr.decRef();
  }

  public synchronized boolean delete(int docID) {
    assert liveDocs != null;
    assert Thread.holdsLock(writer);
    assert docID >= 0 && docID < liveDocs.length() : "out of bounds: docid=" + docID + " liveDocsLength=" + liveDocs.length() + " seg=" + info.info.name + " maxDoc=" + info.info.maxDoc();
    assert !liveDocsShared;
    final boolean didDelete = liveDocs.get(docID);
    if (didDelete) {
      ((MutableBits) liveDocs).clear(docID);
      pendingDeleteCount++;
      //System.out.println("  new del seg=" + info + " docID=" + docID + " pendingDelCount=" + pendingDeleteCount + " totDelCount=" + (info.info.maxDoc()-liveDocs.count()));
    }
    return didDelete;
  }

  // NOTE: removes callers ref
  public synchronized void dropReaders() throws IOException {
    // TODO: can we somehow use IOUtils here...?  problem is
    // we are calling .decRef not .close)...
    if (reader != null) {
      //System.out.println("  pool.drop info=" + info + " rc=" + reader.getRefCount());
      try {
        reader.decRef();
      } finally {
        reader = null;
      }
    }

    decRef();
  }

  /**
   * Returns a ref to a clone. NOTE: you should decRef() the reader when you're
   * done (ie do not call close()).
   */
  public synchronized SegmentReader getReadOnlyClone(IOContext context) throws IOException {
    if (reader == null) {
      getReader(context).decRef();
      assert reader != null;
    }
    // force new liveDocs in initWritableLiveDocs even if it's null
    liveDocsShared = true;
    if (liveDocs != null) {
      return new SegmentReader(reader.getSegmentInfo(), reader, liveDocs, info.info.maxDoc() - info.getDelCount() - pendingDeleteCount);
    } else {
      // liveDocs == null and reader != null. That can only be if there are no deletes
      assert reader.getLiveDocs() == null;
      reader.incRef();
      return reader;
    }
  }

  public synchronized void initWritableLiveDocs() throws IOException {
    assert Thread.holdsLock(writer);
    assert info.info.maxDoc() > 0;
    //System.out.println("initWritableLivedocs seg=" + info + " liveDocs=" + liveDocs + " shared=" + shared);
    if (liveDocsShared) {
      // Copy on write: this means we've cloned a
      // SegmentReader sharing the current liveDocs
      // instance; must now make a private clone so we can
      // change it:
      LiveDocsFormat liveDocsFormat = info.info.getCodec().liveDocsFormat();
      if (liveDocs == null) {
        //System.out.println("create BV seg=" + info);
        liveDocs = liveDocsFormat.newLiveDocs(info.info.maxDoc());
      } else {
        liveDocs = liveDocsFormat.newLiveDocs(liveDocs);
      }
      liveDocsShared = false;
    }
  }

  public synchronized Bits getLiveDocs() {
    assert Thread.holdsLock(writer);
    return liveDocs;
  }

  public synchronized Bits getReadOnlyLiveDocs() {
    //System.out.println("getROLiveDocs seg=" + info);
    assert Thread.holdsLock(writer);
    liveDocsShared = true;
    //if (liveDocs != null) {
    //System.out.println("  liveCount=" + liveDocs.count());
    //}
    return liveDocs;
  }

  public synchronized void dropChanges() {
    // Discard (don't save) changes when we are dropping
    // the reader; this is used only on the sub-readers
    // after a successful merge.  If deletes had
    // accumulated on those sub-readers while the merge
    // is running, by now we have carried forward those
    // deletes onto the newly merged segment, so we can
    // discard them on the sub-readers:
    pendingDeleteCount = 0;
    dropMergingUpdates();
  }

  // Commit live docs (writes new _X_N.del files) and field updates (writes new
  // _X_N updates files) to the directory; returns true if it wrote any file
  // and false if there were no new deletes or updates to write:
  public synchronized boolean writeLiveDocs(Directory dir) throws IOException {
    assert Thread.holdsLock(writer);
    //System.out.println("rld.writeLiveDocs seg=" + info + " pendingDelCount=" + pendingDeleteCount + " numericUpdates=" + numericUpdates);
    if (pendingDeleteCount == 0) {
      return false;
    }
    
    // We have new deletes
    assert liveDocs.length() == info.info.maxDoc();
    
    // Do this so we can delete any created files on
    // exception; this saves all codecs from having to do
    // it:
    TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);
    
    // We can write directly to the actual name (vs to a
    // .tmp & renaming it) because the file is not live
    // until segments file is written:
    boolean success = false;
    try {
      Codec codec = info.info.getCodec();
      codec.liveDocsFormat().writeLiveDocs((MutableBits)liveDocs, trackingDir, info, pendingDeleteCount, IOContext.DEFAULT);
      success = true;
    } finally {
      if (!success) {
        // Advance only the nextWriteDelGen so that a 2nd
        // attempt to write will write to a new file
        info.advanceNextWriteDelGen();
        
        // Delete any partially created file(s):
        for (String fileName : trackingDir.getCreatedFiles()) {
          IOUtils.deleteFilesIgnoringExceptions(dir, fileName);
        }
      }
    }
    
    // If we hit an exc in the line above (eg disk full)
    // then info's delGen remains pointing to the previous
    // (successfully written) del docs:
    info.advanceDelGen();
    info.setDelCount(info.getDelCount() + pendingDeleteCount);
    pendingDeleteCount = 0;
    
    return true;
  }
  
  @SuppressWarnings("synthetic-access")
  private void handleNumericDVUpdates(FieldInfos infos, Map<String,NumericDocValuesFieldUpdates> updates,
      Directory dir, DocValuesFormat dvFormat, final SegmentReader reader, Map<Integer,Set<String>> fieldFiles) throws IOException {
    for (Entry<String,NumericDocValuesFieldUpdates> e : updates.entrySet()) {
      final String field = e.getKey();
      final NumericDocValuesFieldUpdates fieldUpdates = e.getValue();

      final long nextDocValuesGen = info.getNextDocValuesGen();
      final String segmentSuffix = Long.toString(nextDocValuesGen, Character.MAX_RADIX);
      final long estUpdatesSize = fieldUpdates.ramBytesPerDoc() * info.info.maxDoc();
      final IOContext updatesContext = new IOContext(new FlushInfo(info.info.maxDoc(), estUpdatesSize));
      final FieldInfo fieldInfo = infos.fieldInfo(field);
      assert fieldInfo != null;
      fieldInfo.setDocValuesGen(nextDocValuesGen);
      final FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] { fieldInfo });
      // separately also track which files were created for this gen
      final TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);
      final SegmentWriteState state = new SegmentWriteState(null, trackingDir, info.info, fieldInfos, null, updatesContext, segmentSuffix);
      try (final DocValuesConsumer fieldsConsumer = dvFormat.fieldsConsumer(state)) {
        // write the numeric updates to a new gen'd docvalues file
        fieldsConsumer.addNumericField(fieldInfo, new Iterable<Number>() {
          final NumericDocValues currentValues = reader.getNumericDocValues(field);
          final Bits docsWithField = reader.getDocsWithField(field);
          final int maxDoc = reader.maxDoc();
          final NumericDocValuesFieldUpdates.Iterator updatesIter = fieldUpdates.iterator();
          @Override
          public Iterator<Number> iterator() {
            updatesIter.reset();
            return new Iterator<Number>() {

              int curDoc = -1;
              int updateDoc = updatesIter.nextDoc();
              
              @Override
              public boolean hasNext() {
                return curDoc < maxDoc - 1;
              }

              @Override
              public Number next() {
                if (++curDoc >= maxDoc) {
                  throw new NoSuchElementException("no more documents to return values for");
                }
                if (curDoc == updateDoc) { // this document has an updated value
                  Long value = updatesIter.value(); // either null (unset value) or updated value
                  updateDoc = updatesIter.nextDoc(); // prepare for next round
                  return value;
                } else {
                  // no update for this document
                  assert curDoc < updateDoc;
                  if (currentValues != null && docsWithField.get(curDoc)) {
                    // only read the current value if the document had a value before
                    return currentValues.get(curDoc);
                  } else {
                    return null;
                  }
                }
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException("this iterator does not support removing elements");
              }
            };
          }
        });
      }
      info.advanceDocValuesGen();
      assert !fieldFiles.containsKey(fieldInfo.number);
      fieldFiles.put(fieldInfo.number, trackingDir.getCreatedFiles());
    }
  }

  @SuppressWarnings("synthetic-access")
  private void handleBinaryDVUpdates(FieldInfos infos, Map<String,BinaryDocValuesFieldUpdates> updates, 
      TrackingDirectoryWrapper dir, DocValuesFormat dvFormat, final SegmentReader reader, Map<Integer,Set<String>> fieldFiles) throws IOException {
    for (Entry<String,BinaryDocValuesFieldUpdates> e : updates.entrySet()) {
      final String field = e.getKey();
      final BinaryDocValuesFieldUpdates fieldUpdates = e.getValue();

      final long nextDocValuesGen = info.getNextDocValuesGen();
      final String segmentSuffix = Long.toString(nextDocValuesGen, Character.MAX_RADIX);
      final long estUpdatesSize = fieldUpdates.ramBytesPerDoc() * info.info.maxDoc();
      final IOContext updatesContext = new IOContext(new FlushInfo(info.info.maxDoc(), estUpdatesSize));
      final FieldInfo fieldInfo = infos.fieldInfo(field);
      assert fieldInfo != null;
      fieldInfo.setDocValuesGen(nextDocValuesGen);
      final FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] { fieldInfo });
      // separately also track which files were created for this gen
      final TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);
      final SegmentWriteState state = new SegmentWriteState(null, trackingDir, info.info, fieldInfos, null, updatesContext, segmentSuffix);
      try (final DocValuesConsumer fieldsConsumer = dvFormat.fieldsConsumer(state)) {
        // write the binary updates to a new gen'd docvalues file
        fieldsConsumer.addBinaryField(fieldInfo, new Iterable<BytesRef>() {
          final BinaryDocValues currentValues = reader.getBinaryDocValues(field);
          final Bits docsWithField = reader.getDocsWithField(field);
          final int maxDoc = reader.maxDoc();
          final BinaryDocValuesFieldUpdates.Iterator updatesIter = fieldUpdates.iterator();
          @Override
          public Iterator<BytesRef> iterator() {
            updatesIter.reset();
            return new Iterator<BytesRef>() {
              
              int curDoc = -1;
              int updateDoc = updatesIter.nextDoc();
              
              @Override
              public boolean hasNext() {
                return curDoc < maxDoc - 1;
              }
              
              @Override
              public BytesRef next() {
                if (++curDoc >= maxDoc) {
                  throw new NoSuchElementException("no more documents to return values for");
                }
                if (curDoc == updateDoc) { // this document has an updated value
                  BytesRef value = updatesIter.value(); // either null (unset value) or updated value
                  updateDoc = updatesIter.nextDoc(); // prepare for next round
                  return value;
                } else {
                  // no update for this document
                  assert curDoc < updateDoc;
                  if (currentValues != null && docsWithField.get(curDoc)) {
                    // only read the current value if the document had a value before
                    return currentValues.get(curDoc);
                  } else {
                    return null;
                  }
                }
              }
              
              @Override
              public void remove() {
                throw new UnsupportedOperationException("this iterator does not support removing elements");
              }
            };
          }
        });
      }
      info.advanceDocValuesGen();
      assert !fieldFiles.containsKey(fieldInfo.number);
      fieldFiles.put(fieldInfo.number, trackingDir.getCreatedFiles());
    }
  }
  
  private Set<String> writeFieldInfosGen(FieldInfos fieldInfos, Directory dir, DocValuesFormat dvFormat, 
      FieldInfosFormat infosFormat) throws IOException {
    final long nextFieldInfosGen = info.getNextFieldInfosGen();
    final String segmentSuffix = Long.toString(nextFieldInfosGen, Character.MAX_RADIX);
    // we write approximately that many bytes (based on Lucene46DVF):
    // HEADER + FOOTER: 40
    // 90 bytes per-field (over estimating long name and attributes map)
    final long estInfosSize = 40 + 90 * fieldInfos.size();
    final IOContext infosContext = new IOContext(new FlushInfo(info.info.maxDoc(), estInfosSize));
    // separately also track which files were created for this gen
    final TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);
    infosFormat.write(trackingDir, info.info, segmentSuffix, fieldInfos, infosContext);
    info.advanceFieldInfosGen();
    return trackingDir.getCreatedFiles();
  }

  // Writes field updates (new _X_N updates files) to the directory
  public synchronized void writeFieldUpdates(Directory dir, DocValuesFieldUpdates.Container dvUpdates) throws IOException {
    assert Thread.holdsLock(writer);
    //System.out.println("rld.writeFieldUpdates: seg=" + info + " numericFieldUpdates=" + numericFieldUpdates);
    
    assert dvUpdates.any();
    
    // Do this so we can delete any created files on
    // exception; this saves all codecs from having to do
    // it:
    TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);
    
    final Map<Integer,Set<String>> newDVFiles = new HashMap<>();
    Set<String> fieldInfosFiles = null;
    FieldInfos fieldInfos = null;
    boolean success = false;
    try {
      final Codec codec = info.info.getCodec();

      // reader could be null e.g. for a just merged segment (from
      // IndexWriter.commitMergedDeletes).
      final SegmentReader reader = this.reader == null ? new SegmentReader(info, IOContext.READONCE) : this.reader;
      try {
        // clone FieldInfos so that we can update their dvGen separately from
        // the reader's infos and write them to a new fieldInfos_gen file
        FieldInfos.Builder builder = new FieldInfos.Builder(writer.globalFieldNumberMap);
        // cannot use builder.add(reader.getFieldInfos()) because it does not
        // clone FI.attributes as well FI.dvGen
        for (FieldInfo fi : reader.getFieldInfos()) {
          FieldInfo clone = builder.add(fi);
          // copy the stuff FieldInfos.Builder doesn't copy
          for (Entry<String,String> e : fi.attributes().entrySet()) {
            clone.putAttribute(e.getKey(), e.getValue());
          }
          clone.setDocValuesGen(fi.getDocValuesGen());
        }
        // create new fields or update existing ones to have NumericDV type
        for (String f : dvUpdates.numericDVUpdates.keySet()) {
          FieldInfo fieldInfo = builder.getOrAdd(f);
          fieldInfo.setDocValuesType(DocValuesType.NUMERIC);
        }
        // create new fields or update existing ones to have BinaryDV type
        for (String f : dvUpdates.binaryDVUpdates.keySet()) {
          FieldInfo fieldInfo = builder.getOrAdd(f);
          fieldInfo.setDocValuesType(DocValuesType.BINARY);
        }
        
        fieldInfos = builder.finish();
        final DocValuesFormat docValuesFormat = codec.docValuesFormat();
        
//          System.out.println("[" + Thread.currentThread().getName() + "] RLD.writeFieldUpdates: applying numeric updates; seg=" + info + " updates=" + numericFieldUpdates);
        handleNumericDVUpdates(fieldInfos, dvUpdates.numericDVUpdates, trackingDir, docValuesFormat, reader, newDVFiles);
        
//        System.out.println("[" + Thread.currentThread().getName() + "] RAU.writeFieldUpdates: applying binary updates; seg=" + info + " updates=" + dvUpdates.binaryDVUpdates);
        handleBinaryDVUpdates(fieldInfos, dvUpdates.binaryDVUpdates, trackingDir, docValuesFormat, reader, newDVFiles);

//        System.out.println("[" + Thread.currentThread().getName() + "] RAU.writeFieldUpdates: write fieldInfos; seg=" + info);
        fieldInfosFiles = writeFieldInfosGen(fieldInfos, trackingDir, docValuesFormat, codec.fieldInfosFormat());
      } finally {
        if (reader != this.reader) {
//          System.out.println("[" + Thread.currentThread().getName() + "] RLD.writeLiveDocs: closeReader " + reader);
          reader.close();
        }
      }
    
      success = true;
    } finally {
      if (!success) {
        // Advance only the nextWriteFieldInfosGen and nextWriteDocValuesGen, so
        // that a 2nd attempt to write will write to a new file
        info.advanceNextWriteFieldInfosGen();
        info.advanceNextWriteDocValuesGen();
        
        // Delete any partially created file(s):
        for (String fileName : trackingDir.getCreatedFiles()) {
          IOUtils.deleteFilesIgnoringExceptions(dir, fileName);
        }
      }
    }
    
    // copy all the updates to mergingUpdates, so they can later be applied to the merged segment
    if (isMerging) {
      for (Entry<String,NumericDocValuesFieldUpdates> e : dvUpdates.numericDVUpdates.entrySet()) {
        DocValuesFieldUpdates updates = mergingDVUpdates.get(e.getKey());
        if (updates == null) {
          mergingDVUpdates.put(e.getKey(), e.getValue());
        } else {
          updates.merge(e.getValue());
        }
      }
      for (Entry<String,BinaryDocValuesFieldUpdates> e : dvUpdates.binaryDVUpdates.entrySet()) {
        DocValuesFieldUpdates updates = mergingDVUpdates.get(e.getKey());
        if (updates == null) {
          mergingDVUpdates.put(e.getKey(), e.getValue());
        } else {
          updates.merge(e.getValue());
        }
      }
    }
    
    // writing field updates succeeded
    assert fieldInfosFiles != null;
    info.setFieldInfosFiles(fieldInfosFiles);
    
    // update the doc-values updates files. the files map each field to its set
    // of files, hence we copy from the existing map all fields w/ updates that
    // were not updated in this session, and add new mappings for fields that
    // were updated now.
    assert !newDVFiles.isEmpty();
    for (Entry<Integer,Set<String>> e : info.getDocValuesUpdatesFiles().entrySet()) {
      if (!newDVFiles.containsKey(e.getKey())) {
        newDVFiles.put(e.getKey(), e.getValue());
      }
    }
    info.setDocValuesUpdatesFiles(newDVFiles);
    
    // wrote new files, should checkpoint()
    writer.checkpoint();

    // if there is a reader open, reopen it to reflect the updates
    if (reader != null) {
      SegmentReader newReader = new SegmentReader(info, reader, liveDocs, info.info.maxDoc() - info.getDelCount() - pendingDeleteCount);
      boolean reopened = false;
      try {
        reader.decRef();
        reader = newReader;
        reopened = true;
      } finally {
        if (!reopened) {
          newReader.decRef();
        }
      }
    }
  }

  /**
   * Returns a reader for merge. This method applies field updates if there are
   * any and marks that this segment is currently merging.
   */
  synchronized SegmentReader getReaderForMerge(IOContext context) throws IOException {
    assert Thread.holdsLock(writer);
    // must execute these two statements as atomic operation, otherwise we
    // could lose updates if e.g. another thread calls writeFieldUpdates in
    // between, or the updates are applied to the obtained reader, but then
    // re-applied in IW.commitMergedDeletes (unnecessary work and potential
    // bugs).
    isMerging = true;
    return getReader(context);
  }
  
  /**
   * Drops all merging updates. Called from IndexWriter after this segment
   * finished merging (whether successfully or not).
   */
  public synchronized void dropMergingUpdates() {
    mergingDVUpdates.clear();
    isMerging = false;
  }
  
  /** Returns updates that came in while this segment was merging. */
  public synchronized Map<String,DocValuesFieldUpdates> getMergingFieldUpdates() {
    return mergingDVUpdates;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ReadersAndLiveDocs(seg=").append(info);
    sb.append(" pendingDeleteCount=").append(pendingDeleteCount);
    sb.append(" liveDocsShared=").append(liveDocsShared);
    return sb.toString();
  }
  
}
