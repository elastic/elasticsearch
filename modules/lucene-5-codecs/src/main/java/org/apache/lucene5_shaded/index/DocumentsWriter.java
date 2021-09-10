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


import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene5_shaded.analysis.Analyzer;
import org.apache.lucene5_shaded.index.DocumentsWriterFlushQueue.SegmentFlushTicket;
import org.apache.lucene5_shaded.index.DocumentsWriterPerThread.FlushedSegment;
import org.apache.lucene5_shaded.index.DocumentsWriterPerThreadPool.ThreadState;
import org.apache.lucene5_shaded.index.IndexWriter.Event;
import org.apache.lucene5_shaded.search.Query;
import org.apache.lucene5_shaded.store.AlreadyClosedException;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.InfoStream;

/**
 * This class accepts multiple added documents and directly
 * writes segment files.
 *
 * Each added document is passed to the indexing chain,
 * which in turn processes the document into the different
 * codec formats.  Some formats write bytes to files
 * immediately, e.g. stored fields and term vectors, while
 * others are buffered by the indexing chain and written
 * only on flush.
 *
 * Once we have used our allowed RAM buffer, or the number
 * of added docs is large enough (in the case we are
 * flushing by doc count instead of RAM usage), we create a
 * real segment and flush it to the Directory.
 *
 * Threads:
 *
 * Multiple threads are allowed into addDocument at once.
 * There is an initial synchronized call to getThreadState
 * which allocates a ThreadState for this thread.  The same
 * thread will get the same ThreadState over time (thread
 * affinity) so that if there are consistent patterns (for
 * example each thread is indexing a different content
 * source) then we make better use of RAM.  Then
 * processDocument is called on that ThreadState without
 * synchronization (most of the "heavy lifting" is in this
 * call).  Finally the synchronized "finishDocument" is
 * called to flush changes to the directory.
 *
 * When flush is called by IndexWriter we forcefully idle
 * all threads and flush only once they are all idle.  This
 * means you can call flush with a given thread even while
 * other threads are actively adding/deleting documents.
 *
 *
 * Exceptions:
 *
 * Because this class directly updates in-memory posting
 * lists, and flushes stored fields and term vectors
 * directly to files in the directory, there are certain
 * limited times when an exception can corrupt this state.
 * For example, a disk full while flushing stored fields
 * leaves this file in a corrupt state.  Or, an OOM
 * exception while appending to the in-memory posting lists
 * can corrupt that posting list.  We call such exceptions
 * "aborting exceptions".  In these cases we must call
 * abort() to discard all docs added since the last flush.
 *
 * All other exceptions ("non-aborting exceptions") can
 * still partially update the index structures.  These
 * updates are consistent, but, they represent only a part
 * of the document seen up until the exception was hit.
 * When this happens, we immediately mark the document as
 * deleted so that the document is always atomically ("all
 * or none") added to the index.
 */

final class DocumentsWriter implements Closeable, Accountable {
  private final Directory directoryOrig; // no wrapping, for infos
  private final Directory directory;

  private volatile boolean closed;

  private final InfoStream infoStream;

  private final LiveIndexWriterConfig config;

  private final AtomicInteger numDocsInRAM = new AtomicInteger(0);

  // TODO: cut over to BytesRefHash in BufferedDeletes
  volatile DocumentsWriterDeleteQueue deleteQueue = new DocumentsWriterDeleteQueue();
  private final DocumentsWriterFlushQueue ticketQueue = new DocumentsWriterFlushQueue();
  /*
   * we preserve changes during a full flush since IW might not checkout before
   * we release all changes. NRT Readers otherwise suddenly return true from
   * isCurrent while there are actually changes currently committed. See also
   * #anyChanges() & #flushAllThreads
   */
  private volatile boolean pendingChangesInCurrentFullFlush;

  final DocumentsWriterPerThreadPool perThreadPool;
  final FlushPolicy flushPolicy;
  final DocumentsWriterFlushControl flushControl;
  private final IndexWriter writer;
  private final Queue<Event> events;

  
  DocumentsWriter(IndexWriter writer, LiveIndexWriterConfig config, Directory directoryOrig, Directory directory) {
    this.directoryOrig = directoryOrig;
    this.directory = directory;
    this.config = config;
    this.infoStream = config.getInfoStream();
    this.perThreadPool = config.getIndexerThreadPool();
    flushPolicy = config.getFlushPolicy();
    this.writer = writer;
    this.events = new ConcurrentLinkedQueue<>();
    flushControl = new DocumentsWriterFlushControl(this, config, writer.bufferedUpdatesStream);
  }
  
  synchronized boolean deleteQueries(final Query... queries) throws IOException {
    // TODO why is this synchronized?
    final DocumentsWriterDeleteQueue deleteQueue = this.deleteQueue;
    deleteQueue.addDelete(queries);
    flushControl.doOnDelete();
    return applyAllDeletes(deleteQueue);
  }

  // TODO: we could check w/ FreqProxTermsWriter: if the
  // term doesn't exist, don't bother buffering into the
  // per-DWPT map (but still must go into the global map)
  synchronized boolean deleteTerms(final Term... terms) throws IOException {
    // TODO why is this synchronized?
    final DocumentsWriterDeleteQueue deleteQueue = this.deleteQueue;
    deleteQueue.addDelete(terms);
    flushControl.doOnDelete();
    return applyAllDeletes( deleteQueue);
  }

  synchronized boolean updateDocValues(DocValuesUpdate... updates) throws IOException {
    final DocumentsWriterDeleteQueue deleteQueue = this.deleteQueue;
    deleteQueue.addDocValuesUpdates(updates);
    flushControl.doOnDelete();
    return applyAllDeletes(deleteQueue);
  }
  
  DocumentsWriterDeleteQueue currentDeleteSession() {
    return deleteQueue;
  }
  
  private boolean applyAllDeletes(DocumentsWriterDeleteQueue deleteQueue) throws IOException {
    if (flushControl.getAndResetApplyAllDeletes()) {
      if (deleteQueue != null && !flushControl.isFullFlush()) {
        ticketQueue.addDeletes(deleteQueue);
      }
      putEvent(ApplyDeletesEvent.INSTANCE); // apply deletes event forces a purge
      return true;
    }
    return false;
  }
  
  int purgeBuffer(IndexWriter writer, boolean forced) throws IOException {
    if (forced) {
      return ticketQueue.forcePurge(writer);
    } else {
      return ticketQueue.tryPurge(writer);
    }
  }
  

  /** Returns how many docs are currently buffered in RAM. */
  int getNumDocs() {
    return numDocsInRAM.get();
  }

  private void ensureOpen() throws AlreadyClosedException {
    if (closed) {
      throw new AlreadyClosedException("this IndexWriter is closed");
    }
  }

  /** Called if we hit an exception at a bad time (when
   *  updating the index files) and must discard all
   *  currently buffered docs.  This resets our state,
   *  discarding any docs added since last flush. */
  synchronized void abort(IndexWriter writer) {
    assert !Thread.holdsLock(writer) : "IndexWriter lock should never be hold when aborting";
    boolean success = false;
    try {
      deleteQueue.clear();
      if (infoStream.isEnabled("DW")) {
        infoStream.message("DW", "abort");
      }
      final int limit = perThreadPool.getActiveThreadStateCount();
      for (int i = 0; i < limit; i++) {
        final ThreadState perThread = perThreadPool.getThreadState(i);
        perThread.lock();
        try {
          abortThreadState(perThread);
        } finally {
          perThread.unlock();
        }
      }
      flushControl.abortPendingFlushes();
      flushControl.waitForFlush();
      success = true;
    } finally {
      if (infoStream.isEnabled("DW")) {
        infoStream.message("DW", "done abort success=" + success);
      }
    }
  }

  /** Returns how many documents were aborted. */
  synchronized long lockAndAbortAll(IndexWriter indexWriter) {
    assert indexWriter.holdsFullFlushLock();
    if (infoStream.isEnabled("DW")) {
      infoStream.message("DW", "lockAndAbortAll");
    }
    long abortedDocCount = 0;
    boolean success = false;
    try {
      deleteQueue.clear();
      final int limit = perThreadPool.getMaxThreadStates();
      perThreadPool.setAbort();
      for (int i = 0; i < limit; i++) {
        final ThreadState perThread = perThreadPool.getThreadState(i);
        perThread.lock();
        abortedDocCount += abortThreadState(perThread);
      }
      deleteQueue.clear();
      flushControl.abortPendingFlushes();
      flushControl.waitForFlush();
      success = true;
      return abortedDocCount;
    } finally {
      if (infoStream.isEnabled("DW")) {
        infoStream.message("DW", "finished lockAndAbortAll success=" + success);
      }
      if (success == false) {
        // if something happens here we unlock all states again
        unlockAllAfterAbortAll(indexWriter);
      }
    }
  }
  
  /** Returns how many documents were aborted. */
  private int abortThreadState(final ThreadState perThread) {
    assert perThread.isHeldByCurrentThread();
    if (perThread.isInitialized()) { 
      try {
        int abortedDocCount = perThread.dwpt.getNumDocsInRAM();
        subtractFlushedNumDocs(abortedDocCount);
        perThread.dwpt.abort();
        return abortedDocCount;
      } finally {
        flushControl.doOnAbort(perThread);
      }
    } else {
      flushControl.doOnAbort(perThread);
      // This DWPT was never initialized so it has no indexed documents:
      return 0;
    }
  }
  
  synchronized void unlockAllAfterAbortAll(IndexWriter indexWriter) {
    assert indexWriter.holdsFullFlushLock();
    if (infoStream.isEnabled("DW")) {
      infoStream.message("DW", "unlockAll");
    }
    final int limit = perThreadPool.getMaxThreadStates();
    perThreadPool.clearAbort();
    for (int i = 0; i < limit; i++) {
      try {
        final ThreadState perThread = perThreadPool.getThreadState(i);
        if (perThread.isHeldByCurrentThread()) {
          perThread.unlock();
        }
      } catch (Throwable e) {
        if (infoStream.isEnabled("DW")) {
          infoStream.message("DW", "unlockAll: could not unlock state: " + i + " msg:" + e.getMessage());
        }
        // ignore & keep on unlocking
      }
    }
  }

  boolean anyChanges() {
    /*
     * changes are either in a DWPT or in the deleteQueue.
     * yet if we currently flush deletes and / or dwpt there
     * could be a window where all changes are in the ticket queue
     * before they are published to the IW. ie we need to check if the 
     * ticket queue has any tickets.
     */
    boolean anyChanges = numDocsInRAM.get() != 0 || anyDeletions() || ticketQueue.hasTickets() || pendingChangesInCurrentFullFlush;
    if (infoStream.isEnabled("DW") && anyChanges) {
      infoStream.message("DW", "anyChanges? numDocsInRam=" + numDocsInRAM.get()
                         + " deletes=" + anyDeletions() + " hasTickets:"
                         + ticketQueue.hasTickets() + " pendingChangesInFullFlush: "
                         + pendingChangesInCurrentFullFlush);
    }
    return anyChanges;
  }
  
  public int getBufferedDeleteTermsSize() {
    return deleteQueue.getBufferedUpdatesTermsSize();
  }

  //for testing
  public int getNumBufferedDeleteTerms() {
    return deleteQueue.numGlobalTermDeletes();
  }

  public boolean anyDeletions() {
    return deleteQueue.anyChanges();
  }

  @Override
  public void close() {
    closed = true;
    flushControl.setClosed();
  }

  private boolean preUpdate() throws IOException, AbortingException {
    ensureOpen();
    boolean hasEvents = false;
    if (flushControl.anyStalledThreads() || flushControl.numQueuedFlushes() > 0) {
      // Help out flushing any queued DWPTs so we can un-stall:
      if (infoStream.isEnabled("DW")) {
        infoStream.message("DW", "DocumentsWriter has queued dwpt; will hijack this thread to flush pending segment(s)");
      }
      do {
        // Try pick up pending threads here if possible
        DocumentsWriterPerThread flushingDWPT;
        while ((flushingDWPT = flushControl.nextPendingFlush()) != null) {
          // Don't push the delete here since the update could fail!
          hasEvents |= doFlush(flushingDWPT);
        }
  
        if (infoStream.isEnabled("DW") && flushControl.anyStalledThreads()) {
          infoStream.message("DW", "WARNING DocumentsWriter has stalled threads; waiting");
        }
        
        flushControl.waitIfStalled(); // block if stalled
      } while (flushControl.numQueuedFlushes() != 0); // still queued DWPTs try help flushing

      if (infoStream.isEnabled("DW")) {
        infoStream.message("DW", "continue indexing after helping out flushing DocumentsWriter is healthy");
      }
    }
    return hasEvents;
  }

  private boolean postUpdate(DocumentsWriterPerThread flushingDWPT, boolean hasEvents) throws IOException, AbortingException {
    hasEvents |= applyAllDeletes(deleteQueue);
    if (flushingDWPT != null) {
      hasEvents |= doFlush(flushingDWPT);
    } else {
      final DocumentsWriterPerThread nextPendingFlush = flushControl.nextPendingFlush();
      if (nextPendingFlush != null) {
        hasEvents |= doFlush(nextPendingFlush);
      }
    }

    return hasEvents;
  }
  
  private void ensureInitialized(ThreadState state) throws IOException {
    if (state.dwpt == null) {
      final FieldInfos.Builder infos = new FieldInfos.Builder(writer.globalFieldNumberMap);
      state.dwpt = new DocumentsWriterPerThread(writer, writer.newSegmentName(), directoryOrig,
                                                directory, config, infoStream, deleteQueue, infos,
                                                writer.pendingNumDocs, writer.enableTestPoints);
    }
  }

  boolean updateDocuments(final Iterable<? extends Iterable<? extends IndexableField>> docs, final Analyzer analyzer,
                          final Term delTerm) throws IOException, AbortingException {
    boolean hasEvents = preUpdate();

    final ThreadState perThread = flushControl.obtainAndLock();
    final DocumentsWriterPerThread flushingDWPT;
    
    try {
      // This must happen after we've pulled the ThreadState because IW.close
      // waits for all ThreadStates to be released:
      ensureOpen();
      ensureInitialized(perThread);
      assert perThread.isInitialized();
      final DocumentsWriterPerThread dwpt = perThread.dwpt;
      final int dwptNumDocs = dwpt.getNumDocsInRAM();
      try {
        dwpt.updateDocuments(docs, analyzer, delTerm);
      } catch (AbortingException ae) {
        flushControl.doOnAbort(perThread);
        dwpt.abort();
        throw ae;
      } finally {
        // We don't know how many documents were actually
        // counted as indexed, so we must subtract here to
        // accumulate our separate counter:
        numDocsInRAM.addAndGet(dwpt.getNumDocsInRAM() - dwptNumDocs);
      }
      final boolean isUpdate = delTerm != null;
      flushingDWPT = flushControl.doAfterDocument(perThread, isUpdate);
    } finally {
      perThreadPool.release(perThread);
    }

    return postUpdate(flushingDWPT, hasEvents);
  }

  boolean updateDocument(final Iterable<? extends IndexableField> doc, final Analyzer analyzer,
      final Term delTerm) throws IOException, AbortingException {

    boolean hasEvents = preUpdate();

    final ThreadState perThread = flushControl.obtainAndLock();

    final DocumentsWriterPerThread flushingDWPT;
    try {
      // This must happen after we've pulled the ThreadState because IW.close
      // waits for all ThreadStates to be released:
      ensureOpen();
      ensureInitialized(perThread);
      assert perThread.isInitialized();
      final DocumentsWriterPerThread dwpt = perThread.dwpt;
      final int dwptNumDocs = dwpt.getNumDocsInRAM();
      try {
        dwpt.updateDocument(doc, analyzer, delTerm); 
      } catch (AbortingException ae) {
        flushControl.doOnAbort(perThread);
        dwpt.abort();
        throw ae;
      } finally {
        // We don't know whether the document actually
        // counted as being indexed, so we must subtract here to
        // accumulate our separate counter:
        numDocsInRAM.addAndGet(dwpt.getNumDocsInRAM() - dwptNumDocs);
      }
      final boolean isUpdate = delTerm != null;
      flushingDWPT = flushControl.doAfterDocument(perThread, isUpdate);
    } finally {
      perThreadPool.release(perThread);
    }

    return postUpdate(flushingDWPT, hasEvents);
  }

  private boolean doFlush(DocumentsWriterPerThread flushingDWPT) throws IOException, AbortingException {
    boolean hasEvents = false;
    while (flushingDWPT != null) {
      hasEvents = true;
      boolean success = false;
      SegmentFlushTicket ticket = null;
      try {
        assert currentFullFlushDelQueue == null
            || flushingDWPT.deleteQueue == currentFullFlushDelQueue : "expected: "
            + currentFullFlushDelQueue + "but was: " + flushingDWPT.deleteQueue
            + " " + flushControl.isFullFlush();
        /*
         * Since with DWPT the flush process is concurrent and several DWPT
         * could flush at the same time we must maintain the order of the
         * flushes before we can apply the flushed segment and the frozen global
         * deletes it is buffering. The reason for this is that the global
         * deletes mark a certain point in time where we took a DWPT out of
         * rotation and freeze the global deletes.
         * 
         * Example: A flush 'A' starts and freezes the global deletes, then
         * flush 'B' starts and freezes all deletes occurred since 'A' has
         * started. if 'B' finishes before 'A' we need to wait until 'A' is done
         * otherwise the deletes frozen by 'B' are not applied to 'A' and we
         * might miss to deletes documents in 'A'.
         */
        try {
          // Each flush is assigned a ticket in the order they acquire the ticketQueue lock
          ticket = ticketQueue.addFlushTicket(flushingDWPT);
  
          final int flushingDocsInRam = flushingDWPT.getNumDocsInRAM();
          boolean dwptSuccess = false;
          try {
            // flush concurrently without locking
            final FlushedSegment newSegment = flushingDWPT.flush();
            ticketQueue.addSegment(ticket, newSegment);
            dwptSuccess = true;
          } finally {
            subtractFlushedNumDocs(flushingDocsInRam);
            if (!flushingDWPT.pendingFilesToDelete().isEmpty()) {
              putEvent(new DeleteNewFilesEvent(flushingDWPT.pendingFilesToDelete()));
              hasEvents = true;
            }
            if (!dwptSuccess) {
              putEvent(new FlushFailedEvent(flushingDWPT.getSegmentInfo()));
              hasEvents = true;
            }
          }
          // flush was successful once we reached this point - new seg. has been assigned to the ticket!
          success = true;
        } finally {
          if (!success && ticket != null) {
            // In the case of a failure make sure we are making progress and
            // apply all the deletes since the segment flush failed since the flush
            // ticket could hold global deletes see FlushTicket#canPublish()
            ticketQueue.markTicketFailed(ticket);
          }
        }
        /*
         * Now we are done and try to flush the ticket queue if the head of the
         * queue has already finished the flush.
         */
        if (ticketQueue.getTicketCount() >= perThreadPool.getActiveThreadStateCount()) {
          // This means there is a backlog: the one
          // thread in innerPurge can't keep up with all
          // other threads flushing segments.  In this case
          // we forcefully stall the producers.
          putEvent(ForcedPurgeEvent.INSTANCE);
          break;
        }
      } finally {
        flushControl.doAfterFlush(flushingDWPT);
      }
     
      flushingDWPT = flushControl.nextPendingFlush();
    }
    if (hasEvents) {
      putEvent(MergePendingEvent.INSTANCE);
    }
    // If deletes alone are consuming > 1/2 our RAM
    // buffer, force them all to apply now. This is to
    // prevent too-frequent flushing of a long tail of
    // tiny segments:
    final double ramBufferSizeMB = config.getRAMBufferSizeMB();
    if (ramBufferSizeMB != IndexWriterConfig.DISABLE_AUTO_FLUSH &&
        flushControl.getDeleteBytesUsed() > (1024*1024*ramBufferSizeMB/2)) {
      hasEvents = true;
      if (!this.applyAllDeletes(deleteQueue)) {
        if (infoStream.isEnabled("DW")) {
          infoStream.message("DW", String.format(Locale.ROOT, "force apply deletes bytesUsed=%.1f MB vs ramBuffer=%.1f MB",
                                                 flushControl.getDeleteBytesUsed()/(1024.*1024.),
                                                 ramBufferSizeMB));
        }
        putEvent(ApplyDeletesEvent.INSTANCE);
      }
    }

    return hasEvents;
  }
  
  void subtractFlushedNumDocs(int numFlushed) {
    int oldValue = numDocsInRAM.get();
    while (!numDocsInRAM.compareAndSet(oldValue, oldValue - numFlushed)) {
      oldValue = numDocsInRAM.get();
    }
    assert numDocsInRAM.get() >= 0;
  }
  
  // for asserts
  private volatile DocumentsWriterDeleteQueue currentFullFlushDelQueue = null;

  // for asserts
  private synchronized boolean setFlushingDeleteQueue(DocumentsWriterDeleteQueue session) {
    currentFullFlushDelQueue = session;
    return true;
  }
  
  /*
   * FlushAllThreads is synced by IW fullFlushLock. Flushing all threads is a
   * two stage operation; the caller must ensure (in try/finally) that finishFlush
   * is called after this method, to release the flush lock in DWFlushControl
   */
  boolean flushAllThreads()
    throws IOException, AbortingException {
    final DocumentsWriterDeleteQueue flushingDeleteQueue;
    if (infoStream.isEnabled("DW")) {
      infoStream.message("DW", "startFullFlush");
    }
    
    synchronized (this) {
      pendingChangesInCurrentFullFlush = anyChanges();
      flushingDeleteQueue = deleteQueue;
      /* Cutover to a new delete queue.  This must be synced on the flush control
       * otherwise a new DWPT could sneak into the loop with an already flushing
       * delete queue */
      flushControl.markForFullFlush(); // swaps the delQueue synced on FlushControl
      assert setFlushingDeleteQueue(flushingDeleteQueue);
    }
    assert currentFullFlushDelQueue != null;
    assert currentFullFlushDelQueue != deleteQueue;
    
    boolean anythingFlushed = false;
    try {
      DocumentsWriterPerThread flushingDWPT;
      // Help out with flushing:
      while ((flushingDWPT = flushControl.nextPendingFlush()) != null) {
        anythingFlushed |= doFlush(flushingDWPT);
      }
      // If a concurrent flush is still in flight wait for it
      flushControl.waitForFlush();  
      if (!anythingFlushed && flushingDeleteQueue.anyChanges()) { // apply deletes if we did not flush any document
        if (infoStream.isEnabled("DW")) {
          infoStream.message("DW", Thread.currentThread().getName() + ": flush naked frozen global deletes");
        }
        ticketQueue.addDeletes(flushingDeleteQueue);
      } 
      ticketQueue.forcePurge(writer);
      assert !flushingDeleteQueue.anyChanges() && !ticketQueue.hasTickets();
    } finally {
      assert flushingDeleteQueue == currentFullFlushDelQueue;
    }
    return anythingFlushed;
  }
  
  void finishFullFlush(IndexWriter indexWriter, boolean success) {
    assert indexWriter.holdsFullFlushLock();
    try {
      if (infoStream.isEnabled("DW")) {
        infoStream.message("DW", Thread.currentThread().getName() + " finishFullFlush success=" + success);
      }
      assert setFlushingDeleteQueue(null);
      if (success) {
        // Release the flush lock
        flushControl.finishFullFlush();
      } else {
        flushControl.abortFullFlushes();

      }
    } finally {
      pendingChangesInCurrentFullFlush = false;
    }
    
  }

  public LiveIndexWriterConfig getIndexWriterConfig() {
    return config;
  }
  
  private void putEvent(Event event) {
    events.add(event);
  }

  @Override
  public long ramBytesUsed() {
    return flushControl.ramBytesUsed();
  }
  
  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }

  static final class ApplyDeletesEvent implements Event {
    static final Event INSTANCE = new ApplyDeletesEvent();
    private int instCount = 0;
    private ApplyDeletesEvent() {
      assert instCount == 0;
      instCount++;
    }
    
    @Override
    public void process(IndexWriter writer, boolean triggerMerge, boolean forcePurge) throws IOException {
      writer.applyDeletesAndPurge(true); // we always purge!
    }
  }
  
  static final class MergePendingEvent implements Event {
    static final Event INSTANCE = new MergePendingEvent();
    private int instCount = 0; 
    private MergePendingEvent() {
      assert instCount == 0;
      instCount++;
    }
   
    @Override
    public void process(IndexWriter writer, boolean triggerMerge, boolean forcePurge) throws IOException {
      writer.doAfterSegmentFlushed(triggerMerge, forcePurge);
    }
  }
  
  static final class ForcedPurgeEvent implements Event {
    static final Event INSTANCE = new ForcedPurgeEvent();
    private int instCount = 0;
    private ForcedPurgeEvent() {
      assert instCount == 0;
      instCount++;
    }
    
    @Override
    public void process(IndexWriter writer, boolean triggerMerge, boolean forcePurge) throws IOException {
      writer.purge(true);
    }
  }
  
  static class FlushFailedEvent implements Event {
    private final SegmentInfo info;
    
    public FlushFailedEvent(SegmentInfo info) {
      this.info = info;
    }
    
    @Override
    public void process(IndexWriter writer, boolean triggerMerge, boolean forcePurge) throws IOException {
      writer.flushFailed(info);
    }
  }
  
  static class DeleteNewFilesEvent implements Event {
    private final Collection<String>  files;
    
    public DeleteNewFilesEvent(Collection<String>  files) {
      this.files = files;
    }
    
    @Override
    public void process(IndexWriter writer, boolean triggerMerge, boolean forcePurge) throws IOException {
      writer.deleteNewFiles(files);
    }
  }

  public Queue<Event> eventQueue() {
    return events;
  }
}
