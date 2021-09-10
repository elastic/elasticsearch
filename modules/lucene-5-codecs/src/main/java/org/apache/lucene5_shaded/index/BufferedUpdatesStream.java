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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene5_shaded.search.DocIdSetIterator;
import org.apache.lucene5_shaded.search.IndexSearcher;
import org.apache.lucene5_shaded.search.Query;
import org.apache.lucene5_shaded.search.Scorer;
import org.apache.lucene5_shaded.search.Weight;
import org.apache.lucene5_shaded.store.IOContext;
import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.IOUtils;
import org.apache.lucene5_shaded.util.InfoStream;
import org.apache.lucene5_shaded.util.PriorityQueue;

/* Tracks the stream of {@link BufferedDeletes}.
 * When DocumentsWriterPerThread flushes, its buffered
 * deletes and updates are appended to this stream.  We later
 * apply them (resolve them to the actual
 * docIDs, per segment) when a merge is started
 * (only to the to-be-merged segments).  We
 * also apply to all segments when NRT reader is pulled,
 * commit/close is called, or when too many deletes or  updates are
 * buffered and must be flushed (by RAM usage or by count).
 *
 * Each packet is assigned a generation, and each flushed or
 * merged segment is also assigned a generation, so we can
 * track which BufferedDeletes packets to apply to any given
 * segment. */

class BufferedUpdatesStream implements Accountable {

  // TODO: maybe linked list?
  private final List<FrozenBufferedUpdates> updates = new ArrayList<>();

  // Starts at 1 so that SegmentInfos that have never had
  // deletes applied (whose bufferedDelGen defaults to 0)
  // will be correct:
  private long nextGen = 1;

  // used only by assert
  private BytesRef lastDeleteTerm;

  private final InfoStream infoStream;
  private final AtomicLong bytesUsed = new AtomicLong();
  private final AtomicInteger numTerms = new AtomicInteger();

  public BufferedUpdatesStream(InfoStream infoStream) {
    this.infoStream = infoStream;
  }

  // Appends a new packet of buffered deletes to the stream,
  // setting its generation:
  public synchronized long push(FrozenBufferedUpdates packet) {
    /*
     * The insert operation must be atomic. If we let threads increment the gen
     * and push the packet afterwards we risk that packets are out of order.
     * With DWPT this is possible if two or more flushes are racing for pushing
     * updates. If the pushed packets get our of order would loose documents
     * since deletes are applied to the wrong segments.
     */
    packet.setDelGen(nextGen++);
    assert packet.any();
    assert checkDeleteStats();
    assert packet.delGen() < nextGen;
    assert updates.isEmpty() || updates.get(updates.size()-1).delGen() < packet.delGen() : "Delete packets must be in order";
    updates.add(packet);
    numTerms.addAndGet(packet.numTermDeletes);
    bytesUsed.addAndGet(packet.bytesUsed);
    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD", "push deletes " + packet + " segmentPrivate?=" + packet.isSegmentPrivate + " delGen=" + packet.delGen() + " packetCount=" + updates.size() + " totBytesUsed=" + bytesUsed.get());
    }
    assert checkDeleteStats();
    return packet.delGen();
  }

  public synchronized void clear() {
    updates.clear();
    nextGen = 1;
    numTerms.set(0);
    bytesUsed.set(0);
  }

  public boolean any() {
    return bytesUsed.get() != 0;
  }

  public int numTerms() {
    return numTerms.get();
  }

  @Override
  public long ramBytesUsed() {
    return bytesUsed.get();
  }
  
  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }

  public static class ApplyDeletesResult {
    
    // True if any actual deletes took place:
    public final boolean anyDeletes;

    // Current gen, for the merged segment:
    public final long gen;

    // If non-null, contains segments that are 100% deleted
    public final List<SegmentCommitInfo> allDeleted;

    ApplyDeletesResult(boolean anyDeletes, long gen, List<SegmentCommitInfo> allDeleted) {
      this.anyDeletes = anyDeletes;
      this.gen = gen;
      this.allDeleted = allDeleted;
    }
  }

  // Sorts SegmentInfos from smallest to biggest bufferedDelGen:
  private static final Comparator<SegmentCommitInfo> sortSegInfoByDelGen = new Comparator<SegmentCommitInfo>() {
    @Override
    public int compare(SegmentCommitInfo si1, SegmentCommitInfo si2) {
      return Long.compare(si1.getBufferedDeletesGen(), si2.getBufferedDeletesGen());
    }
  };
  
  /** Resolves the buffered deleted Term/Query/docIDs, into
   *  actual deleted docIDs in the liveDocs MutableBits for
   *  each SegmentReader. */
  public synchronized ApplyDeletesResult applyDeletesAndUpdates(IndexWriter.ReaderPool pool, List<SegmentCommitInfo> infos) throws IOException {
    final long t0 = System.currentTimeMillis();

    final long gen = nextGen++;

    if (infos.size() == 0) {
      return new ApplyDeletesResult(false, gen, null);
    }

    // We only init these on demand, when we find our first deletes that need to be applied:
    SegmentState[] segStates = null;

    long totDelCount = 0;
    long totTermVisitedCount = 0;

    boolean success = false;

    ApplyDeletesResult result = null;

    try {
      if (infoStream.isEnabled("BD")) {
        infoStream.message("BD", String.format(Locale.ROOT, "applyDeletes: open segment readers took %d msec", System.currentTimeMillis()-t0));
      }

      assert checkDeleteStats();

      if (!any()) {
        if (infoStream.isEnabled("BD")) {
          infoStream.message("BD", "applyDeletes: no segments; skipping");
        }
        return new ApplyDeletesResult(false, gen, null);
      }

      if (infoStream.isEnabled("BD")) {
        infoStream.message("BD", "applyDeletes: infos=" + infos + " packetCount=" + updates.size());
      }

      infos = sortByDelGen(infos);

      CoalescedUpdates coalescedUpdates = null;
      int infosIDX = infos.size()-1;
      int delIDX = updates.size()-1;

      // Backwards merge sort the segment delGens with the packet delGens in the buffered stream:
      while (infosIDX >= 0) {
        final FrozenBufferedUpdates packet = delIDX >= 0 ? updates.get(delIDX) : null;
        final SegmentCommitInfo info = infos.get(infosIDX);
        final long segGen = info.getBufferedDeletesGen();

        if (packet != null && segGen < packet.delGen()) {
          if (!packet.isSegmentPrivate && packet.any()) {
            /*
             * Only coalesce if we are NOT on a segment private del packet: the segment private del packet
             * must only apply to segments with the same delGen.  Yet, if a segment is already deleted
             * from the SI since it had no more documents remaining after some del packets younger than
             * its segPrivate packet (higher delGen) have been applied, the segPrivate packet has not been
             * removed.
             */
            if (coalescedUpdates == null) {
              coalescedUpdates = new CoalescedUpdates();
            }
            coalescedUpdates.update(packet);
          }

          delIDX--;
        } else if (packet != null && segGen == packet.delGen()) {
          assert packet.isSegmentPrivate : "Packet and Segments deletegen can only match on a segment private del packet gen=" + segGen;

          if (segStates == null) {
            segStates = openSegmentStates(pool, infos);
          }

          SegmentState segState = segStates[infosIDX];

          // Lock order: IW -> BD -> RP
          assert pool.infoIsLive(info);
          int delCount = 0;
          final DocValuesFieldUpdates.Container dvUpdates = new DocValuesFieldUpdates.Container();

          // first apply segment-private deletes/updates
          delCount += applyQueryDeletes(packet.queriesIterable(), segState);
          applyDocValuesUpdates(Arrays.asList(packet.numericDVUpdates), segState, dvUpdates);
          applyDocValuesUpdates(Arrays.asList(packet.binaryDVUpdates), segState, dvUpdates);

          // ... then coalesced deletes/updates, so that if there is an update that appears in both, the coalesced updates (carried from
          // updates ahead of the segment-privates ones) win:
          if (coalescedUpdates != null) {
            delCount += applyQueryDeletes(coalescedUpdates.queriesIterable(), segState);
            applyDocValuesUpdatesList(coalescedUpdates.numericDVUpdates, segState, dvUpdates);
            applyDocValuesUpdatesList(coalescedUpdates.binaryDVUpdates, segState, dvUpdates);
          }
          if (dvUpdates.any()) {
            segState.rld.writeFieldUpdates(info.info.dir, dvUpdates);
          }

          totDelCount += delCount;

          /*
           * Since we are on a segment private del packet we must not
           * update the coalescedUpdates here! We can simply advance to the 
           * next packet and seginfo.
           */
          delIDX--;
          infosIDX--;

        } else {
          if (coalescedUpdates != null) {
            if (segStates == null) {
              segStates = openSegmentStates(pool, infos);
            }
            SegmentState segState = segStates[infosIDX];
            // Lock order: IW -> BD -> RP
            assert pool.infoIsLive(info);
            int delCount = 0;
            delCount += applyQueryDeletes(coalescedUpdates.queriesIterable(), segState);
            DocValuesFieldUpdates.Container dvUpdates = new DocValuesFieldUpdates.Container();
            applyDocValuesUpdatesList(coalescedUpdates.numericDVUpdates, segState, dvUpdates);
            applyDocValuesUpdatesList(coalescedUpdates.binaryDVUpdates, segState, dvUpdates);
            if (dvUpdates.any()) {
              segState.rld.writeFieldUpdates(info.info.dir, dvUpdates);
            }

            totDelCount += delCount;
          }

          infosIDX--;
        }
      }

      // Now apply all term deletes:
      if (coalescedUpdates != null && coalescedUpdates.totalTermCount != 0) {
        if (segStates == null) {
          segStates = openSegmentStates(pool, infos);
        }
        totTermVisitedCount += applyTermDeletes(coalescedUpdates, segStates);
      }

      assert checkDeleteStats();

      success = true;

    } finally {
      if (segStates != null) {
        result = closeSegmentStates(pool, segStates, success, gen);
      }
    }

    if (result == null) {
      result = new ApplyDeletesResult(false, gen, null);      
    }

    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD",
                         String.format(Locale.ROOT,
                                       "applyDeletes took %d msec for %d segments, %d newly deleted docs (query deletes), %d visited terms, allDeleted=%s",
                                       System.currentTimeMillis()-t0, infos.size(), totDelCount, totTermVisitedCount, result.allDeleted));
    }

    return result;
  }

  private List<SegmentCommitInfo> sortByDelGen(List<SegmentCommitInfo> infos) {
    infos = new ArrayList<>(infos);
    // Smaller delGens come first:
    Collections.sort(infos, sortSegInfoByDelGen);
    return infos;
  }

  synchronized long getNextGen() {
    return nextGen++;
  }

  // Lock order IW -> BD
  /* Removes any BufferedDeletes that we no longer need to
   * store because all segments in the index have had the
   * deletes applied. */
  public synchronized void prune(SegmentInfos segmentInfos) {
    assert checkDeleteStats();
    long minGen = Long.MAX_VALUE;
    for(SegmentCommitInfo info : segmentInfos) {
      minGen = Math.min(info.getBufferedDeletesGen(), minGen);
    }

    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD", "prune sis=" + segmentInfos + " minGen=" + minGen + " packetCount=" + updates.size());
    }
    final int limit = updates.size();
    for(int delIDX=0;delIDX<limit;delIDX++) {
      if (updates.get(delIDX).delGen() >= minGen) {
        prune(delIDX);
        assert checkDeleteStats();
        return;
      }
    }

    // All deletes pruned
    prune(limit);
    assert !any();
    assert checkDeleteStats();
  }

  private synchronized void prune(int count) {
    if (count > 0) {
      if (infoStream.isEnabled("BD")) {
        infoStream.message("BD", "pruneDeletes: prune " + count + " packets; " + (updates.size() - count) + " packets remain");
      }
      for(int delIDX=0;delIDX<count;delIDX++) {
        final FrozenBufferedUpdates packet = updates.get(delIDX);
        numTerms.addAndGet(-packet.numTermDeletes);
        assert numTerms.get() >= 0;
        bytesUsed.addAndGet(-packet.bytesUsed);
        assert bytesUsed.get() >= 0;
      }
      updates.subList(0, count).clear();
    }
  }

  static class SegmentState {
    final long delGen;
    final ReadersAndUpdates rld;
    final SegmentReader reader;
    final int startDelCount;

    TermsEnum termsEnum;
    PostingsEnum postingsEnum;
    BytesRef term;
    boolean any;

    public SegmentState(IndexWriter.ReaderPool pool, SegmentCommitInfo info) throws IOException {
      rld = pool.get(info, true);
      startDelCount = rld.getPendingDeleteCount();
      reader = rld.getReader(IOContext.READ);
      delGen = info.getBufferedDeletesGen();
    }

    public void finish(IndexWriter.ReaderPool pool) throws IOException {
      try {
        rld.release(reader);
      } finally {
        pool.release(rld);
      }
    }
  }

  /** Does a merge sort by current term across all segments. */
  static class SegmentQueue extends PriorityQueue<SegmentState> {
    public SegmentQueue(int size) {
      super(size);
    }

    @Override
    protected boolean lessThan(SegmentState a, SegmentState b) {
      return a.term.compareTo(b.term) < 0;
    }
  }

  /** Opens SegmentReader and inits SegmentState for each segment. */
  private SegmentState[] openSegmentStates(IndexWriter.ReaderPool pool, List<SegmentCommitInfo> infos) throws IOException {
    int numReaders = infos.size();
    SegmentState[] segStates = new SegmentState[numReaders];
    boolean success = false;
    try {
      for(int i=0;i<numReaders;i++) {
        segStates[i] = new SegmentState(pool, infos.get(i));
      }
      success = true;
    } finally {
      if (success == false) {
        for(int j=0;j<numReaders;j++) {
          if (segStates[j] != null) {
            try {
              segStates[j].finish(pool);
            } catch (Throwable th) {
              // suppress so we keep throwing original exc
            }
          }
        }
      }
    }

    return segStates;
  }

  /** Close segment states previously opened with openSegmentStates. */
  private ApplyDeletesResult closeSegmentStates(IndexWriter.ReaderPool pool, SegmentState[] segStates, boolean success, long gen) throws IOException {
    int numReaders = segStates.length;
    Throwable firstExc = null;
    List<SegmentCommitInfo> allDeleted = null;
    long totDelCount = 0;
    for (int j=0;j<numReaders;j++) {
      SegmentState segState = segStates[j];
      if (success) {
        totDelCount += segState.rld.getPendingDeleteCount() - segState.startDelCount;
        segState.reader.getSegmentInfo().setBufferedDeletesGen(gen);
        int fullDelCount = segState.rld.info.getDelCount() + segState.rld.getPendingDeleteCount();
        assert fullDelCount <= segState.rld.info.info.maxDoc();
        if (fullDelCount == segState.rld.info.info.maxDoc()) {
          if (allDeleted == null) {
            allDeleted = new ArrayList<>();
          }
          allDeleted.add(segState.reader.getSegmentInfo());
        }
      }
      try {
        segStates[j].finish(pool);
      } catch (Throwable th) {
        if (firstExc != null) {
          firstExc = th;
        }
      }
    }

    if (success) {
      // Does nothing if firstExc is null:
      IOUtils.reThrow(firstExc);
    }

    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD", "applyDeletes: " + totDelCount + " new deleted documents");
    }

    return new ApplyDeletesResult(totDelCount > 0, gen, allDeleted);      
  }

  /** Merge sorts the deleted terms and all segments to resolve terms to docIDs for deletion. */
  private synchronized long applyTermDeletes(CoalescedUpdates updates, SegmentState[] segStates) throws IOException {

    long startNS = System.nanoTime();

    int numReaders = segStates.length;

    long delTermVisitedCount = 0;
    long segTermVisitedCount = 0;

    FieldTermIterator iter = updates.termIterator();

    String field = null;
    SegmentQueue queue = null;

    BytesRef term;

    while ((term = iter.next()) != null) {

      if (iter.field() != field) {
        // field changed
        field = iter.field();

        queue = new SegmentQueue(numReaders);

        long segTermCount = 0;
        for(int i=0;i<numReaders;i++) {
          SegmentState state = segStates[i];
          Terms terms = state.reader.fields().terms(field);
          if (terms != null) {
            segTermCount += terms.size();
            state.termsEnum = terms.iterator();
            state.term = state.termsEnum.next();
            if (state.term != null) {
              queue.add(state);
            }
          }
        }

        assert checkDeleteTerm(null);
      }

      assert checkDeleteTerm(term);

      delTermVisitedCount++;

      long delGen = iter.delGen();

      while (queue.size() != 0) {

        // Get next term merged across all segments
        SegmentState state = queue.top();
        segTermVisitedCount++;

        int cmp = term.compareTo(state.term);

        if (cmp < 0) {
          break;
        } else if (cmp == 0) {
          // fall through
        } else {
          TermsEnum.SeekStatus status = state.termsEnum.seekCeil(term);
          if (status == TermsEnum.SeekStatus.FOUND) {
            // fallthrough
          } else {
            if (status == TermsEnum.SeekStatus.NOT_FOUND) {
              state.term = state.termsEnum.term();
              queue.updateTop();
            } else {
              // No more terms in this segment
              queue.pop();
            }

            continue;
          }
        }

        assert state.delGen != delGen;

        if (state.delGen < delGen) {

          // we don't need term frequencies for this
          final Bits acceptDocs = state.rld.getLiveDocs();
          state.postingsEnum = state.termsEnum.postings(state.postingsEnum, PostingsEnum.NONE);

          assert state.postingsEnum != null;

          while (true) {
            final int docID = state.postingsEnum.nextDoc();
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
              break;
            }
            if (acceptDocs != null && acceptDocs.get(docID) == false) {
              continue;
            }
            if (!state.any) {
              state.rld.initWritableLiveDocs();
              state.any = true;
            }

            // NOTE: there is no limit check on the docID
            // when deleting by Term (unlike by Query)
            // because on flush we apply all Term deletes to
            // each segment.  So all Term deleting here is
            // against prior segments:
            state.rld.delete(docID);
          }
        }

        state.term = state.termsEnum.next();
        if (state.term == null) {
          queue.pop();
        } else {
          queue.updateTop();
        }
      }
    }

    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD",
                         String.format(Locale.ROOT, "applyTermDeletes took %.1f msec for %d segments and %d packets; %d del terms visited; %d seg terms visited",
                                       (System.nanoTime()-startNS)/1000000.,
                                       numReaders,
                                       updates.terms.size(),
                                       delTermVisitedCount, segTermVisitedCount));
    }

    return delTermVisitedCount;
  }

  private synchronized void applyDocValuesUpdatesList(List<List<DocValuesUpdate>> updates, 
      SegmentState segState, DocValuesFieldUpdates.Container dvUpdatesContainer) throws IOException {
    // we walk backwards through the segments, appending deletion packets to the coalesced updates, so we must apply the packets in reverse
    // so that newer packets override older ones:
    for(int idx=updates.size()-1;idx>=0;idx--) {
      applyDocValuesUpdates(updates.get(idx), segState, dvUpdatesContainer);
    }
  }

  // DocValues updates
  private synchronized void applyDocValuesUpdates(Iterable<? extends DocValuesUpdate> updates, 
      SegmentState segState, DocValuesFieldUpdates.Container dvUpdatesContainer) throws IOException {
    Fields fields = segState.reader.fields();

    // TODO: we can process the updates per DV field, from last to first so that
    // if multiple terms affect same document for the same field, we add an update
    // only once (that of the last term). To do that, we can keep a bitset which
    // marks which documents have already been updated. So e.g. if term T1
    // updates doc 7, and then we process term T2 and it updates doc 7 as well,
    // we don't apply the update since we know T1 came last and therefore wins
    // the update.
    // We can also use that bitset as 'liveDocs' to pass to TermEnum.docs(), so
    // that these documents aren't even returned.
    
    String currentField = null;
    TermsEnum termsEnum = null;
    PostingsEnum postingsEnum = null;
    
    for (DocValuesUpdate update : updates) {
      Term term = update.term;
      int limit = update.docIDUpto;
      
      // TODO: we traverse the terms in update order (not term order) so that we
      // apply the updates in the correct order, i.e. if two terms udpate the
      // same document, the last one that came in wins, irrespective of the
      // terms lexical order.
      // we can apply the updates in terms order if we keep an updatesGen (and
      // increment it with every update) and attach it to each NumericUpdate. Note
      // that we cannot rely only on docIDUpto because an app may send two updates
      // which will get same docIDUpto, yet will still need to respect the order
      // those updates arrived.
      
      if (!term.field().equals(currentField)) {
        // if we change the code to process updates in terms order, enable this assert
//        assert currentField == null || currentField.compareTo(term.field()) < 0;
        currentField = term.field();
        Terms terms = fields.terms(currentField);
        if (terms != null) {
          termsEnum = terms.iterator();
        } else {
          termsEnum = null;
        }
      }

      if (termsEnum == null) {
        // no terms in this field
        continue;
      }

      if (termsEnum.seekExact(term.bytes())) {
        // we don't need term frequencies for this
        final Bits acceptDocs = segState.rld.getLiveDocs();
        postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);

        DocValuesFieldUpdates dvUpdates = dvUpdatesContainer.getUpdates(update.field, update.type);
        if (dvUpdates == null) {
          dvUpdates = dvUpdatesContainer.newUpdates(update.field, update.type, segState.reader.maxDoc());
        }
        int doc;
        while ((doc = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          if (doc >= limit) {
            break; // no more docs that can be updated for this term
          }
          if (acceptDocs != null && acceptDocs.get(doc) == false) {
            continue;
          }
          dvUpdates.add(doc, update.value);
        }
      }
    }
  }
  
  public static class QueryAndLimit {
    public final Query query;
    public final int limit;
    public QueryAndLimit(Query query, int limit) {
      this.query = query;
      this.limit = limit;
    }
  }

  // Delete by query
  private static long applyQueryDeletes(Iterable<QueryAndLimit> queriesIter, SegmentState segState) throws IOException {
    long delCount = 0;
    final LeafReaderContext readerContext = segState.reader.getContext();
    for (QueryAndLimit ent : queriesIter) {
      Query query = ent.query;
      int limit = ent.limit;
      final IndexSearcher searcher = new IndexSearcher(readerContext.reader());
      searcher.setQueryCache(null);
      final Weight weight = searcher.createNormalizedWeight(query, false);
      final Scorer scorer = weight.scorer(readerContext);
      if (scorer != null) {
        final DocIdSetIterator it = scorer.iterator();
        final Bits liveDocs = readerContext.reader().getLiveDocs();
        while (true)  {
          int doc = it.nextDoc();
          if (doc >= limit) {
            break;
          }
          if (liveDocs != null && liveDocs.get(doc) == false) {
            continue;
          }

          if (!segState.any) {
            segState.rld.initWritableLiveDocs();
            segState.any = true;
          }
          if (segState.rld.delete(doc)) {
            delCount++;
          }
        }
      }
    }

    return delCount;
  }

  // used only by assert
  private boolean checkDeleteTerm(BytesRef term) {
    if (term != null) {
      assert lastDeleteTerm == null || term.compareTo(lastDeleteTerm) >= 0: "lastTerm=" + lastDeleteTerm + " vs term=" + term;
    }
    // TODO: we re-use term now in our merged iterable, but we shouldn't clone, instead copy for this assert
    lastDeleteTerm = term == null ? null : BytesRef.deepCopyOf(term);
    return true;
  }

  // only for assert
  private boolean checkDeleteStats() {
    int numTerms2 = 0;
    long bytesUsed2 = 0;
    for(FrozenBufferedUpdates packet : updates) {
      numTerms2 += packet.numTermDeletes;
      bytesUsed2 += packet.bytesUsed;
    }
    assert numTerms2 == numTerms.get(): "numTerms2=" + numTerms2 + " vs " + numTerms.get();
    assert bytesUsed2 == bytesUsed.get(): "bytesUsed2=" + bytesUsed2 + " vs " + bytesUsed;
    return true;
  }
}
