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


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene5_shaded.index.DocumentsWriterPerThreadPool.ThreadState;
import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.InfoStream;
import org.apache.lucene5_shaded.util.ThreadInterruptedException;

/**
 * This class controls {@link DocumentsWriterPerThread} flushing during
 * indexing. It tracks the memory consumption per
 * {@link DocumentsWriterPerThread} and uses a configured {@link FlushPolicy} to
 * decide if a {@link DocumentsWriterPerThread} must flush.
 * <p>
 * In addition to the {@link FlushPolicy} the flush control might set certain
 * {@link DocumentsWriterPerThread} as flush pending iff a
 * {@link DocumentsWriterPerThread} exceeds the
 * {@link IndexWriterConfig#getRAMPerThreadHardLimitMB()} to prevent address
 * space exhaustion.
 */
final class DocumentsWriterFlushControl implements Accountable {

  private final long hardMaxBytesPerDWPT;
  private long activeBytes = 0;
  private long flushBytes = 0;
  private volatile int numPending = 0;
  private int numDocsSinceStalled = 0; // only with assert
  final AtomicBoolean flushDeletes = new AtomicBoolean(false);
  private boolean fullFlush = false;
  private final Queue<DocumentsWriterPerThread> flushQueue = new LinkedList<>();
  // only for safety reasons if a DWPT is close to the RAM limit
  private final Queue<BlockedFlush> blockedFlushes = new LinkedList<>();
  private final IdentityHashMap<DocumentsWriterPerThread, Long> flushingWriters = new IdentityHashMap<>();


  double maxConfiguredRamBuffer = 0;
  long peakActiveBytes = 0;// only with assert
  long peakFlushBytes = 0;// only with assert
  long peakNetBytes = 0;// only with assert
  long peakDelta = 0; // only with assert
  boolean flushByRAMWasDisabled; // only with assert
  final DocumentsWriterStallControl stallControl;
  private final DocumentsWriterPerThreadPool perThreadPool;
  private final FlushPolicy flushPolicy;
  private boolean closed = false;
  private final DocumentsWriter documentsWriter;
  private final LiveIndexWriterConfig config;
  private final BufferedUpdatesStream bufferedUpdatesStream;
  private final InfoStream infoStream;

  DocumentsWriterFlushControl(DocumentsWriter documentsWriter, LiveIndexWriterConfig config, BufferedUpdatesStream bufferedUpdatesStream) {
    this.infoStream = config.getInfoStream();
    this.stallControl = new DocumentsWriterStallControl(config);
    this.perThreadPool = documentsWriter.perThreadPool;
    this.flushPolicy = documentsWriter.flushPolicy;
    this.config = config;
    this.hardMaxBytesPerDWPT = config.getRAMPerThreadHardLimitMB() * 1024 * 1024;
    this.documentsWriter = documentsWriter;
    this.bufferedUpdatesStream = bufferedUpdatesStream;
  }

  public synchronized long activeBytes() {
    return activeBytes;
  }

  public synchronized long flushBytes() {
    return flushBytes;
  }

  public synchronized long netBytes() {
    return flushBytes + activeBytes;
  }
  
  private long stallLimitBytes() {
    final double maxRamMB = config.getRAMBufferSizeMB();
    return maxRamMB != IndexWriterConfig.DISABLE_AUTO_FLUSH ? (long)(2 * (maxRamMB * 1024 * 1024)) : Long.MAX_VALUE;
  }
  
  private boolean assertMemory() {
    final double maxRamMB = config.getRAMBufferSizeMB();
    // We can only assert if we have always been flushing by RAM usage; otherwise the assert will false trip if e.g. the
    // flush-by-doc-count * doc size was large enough to use far more RAM than the sudden change to IWC's maxRAMBufferSizeMB:
    if (maxRamMB != IndexWriterConfig.DISABLE_AUTO_FLUSH && flushByRAMWasDisabled == false) {
      // for this assert we must be tolerant to ram buffer changes!
      maxConfiguredRamBuffer = Math.max(maxRamMB, maxConfiguredRamBuffer);
      final long ram = flushBytes + activeBytes;
      final long ramBufferBytes = (long) (maxConfiguredRamBuffer * 1024 * 1024);
      // take peakDelta into account - worst case is that all flushing, pending and blocked DWPT had maxMem and the last doc had the peakDelta
      
      // 2 * ramBufferBytes -> before we stall we need to cross the 2xRAM Buffer border this is still a valid limit
      // (numPending + numFlushingDWPT() + numBlockedFlushes()) * peakDelta) -> those are the total number of DWPT that are not active but not yet fully flushed
      // all of them could theoretically be taken out of the loop once they crossed the RAM buffer and the last document was the peak delta
      // (numDocsSinceStalled * peakDelta) -> at any given time there could be n threads in flight that crossed the stall control before we reached the limit and each of them could hold a peak document
      final long expected = (2 * ramBufferBytes) + ((numPending + numFlushingDWPT() + numBlockedFlushes()) * peakDelta) + (numDocsSinceStalled * peakDelta);
      // the expected ram consumption is an upper bound at this point and not really the expected consumption
      if (peakDelta < (ramBufferBytes >> 1)) {
        /*
         * if we are indexing with very low maxRamBuffer like 0.1MB memory can
         * easily overflow if we check out some DWPT based on docCount and have
         * several DWPT in flight indexing large documents (compared to the ram
         * buffer). This means that those DWPT and their threads will not hit
         * the stall control before asserting the memory which would in turn
         * fail. To prevent this we only assert if the the largest document seen
         * is smaller than the 1/2 of the maxRamBufferMB
         */
        assert ram <= expected : "actual mem: " + ram + " byte, expected mem: " + expected
            + " byte, flush mem: " + flushBytes + ", active mem: " + activeBytes
            + ", pending DWPT: " + numPending + ", flushing DWPT: "
            + numFlushingDWPT() + ", blocked DWPT: " + numBlockedFlushes()
            + ", peakDelta mem: " + peakDelta + " bytes, ramBufferBytes=" + ramBufferBytes
            + ", maxConfiguredRamBuffer=" + maxConfiguredRamBuffer;
      }
    } else {
      flushByRAMWasDisabled = true;
    }
    return true;
  }

  private void commitPerThreadBytes(ThreadState perThread) {
    final long delta = perThread.dwpt.bytesUsed()
        - perThread.bytesUsed;
    perThread.bytesUsed += delta;
    /*
     * We need to differentiate here if we are pending since setFlushPending
     * moves the perThread memory to the flushBytes and we could be set to
     * pending during a delete
     */
    if (perThread.flushPending) {
      flushBytes += delta;
    } else {
      activeBytes += delta;
    }
    assert updatePeaks(delta);
  }

  // only for asserts
  private boolean updatePeaks(long delta) {
    peakActiveBytes = Math.max(peakActiveBytes, activeBytes);
    peakFlushBytes = Math.max(peakFlushBytes, flushBytes);
    peakNetBytes = Math.max(peakNetBytes, netBytes());
    peakDelta = Math.max(peakDelta, delta);
    
    return true;
  }

  synchronized DocumentsWriterPerThread doAfterDocument(ThreadState perThread,
      boolean isUpdate) {
    try {
      commitPerThreadBytes(perThread);
      if (!perThread.flushPending) {
        if (isUpdate) {
          flushPolicy.onUpdate(this, perThread);
        } else {
          flushPolicy.onInsert(this, perThread);
        }
        if (!perThread.flushPending && perThread.bytesUsed > hardMaxBytesPerDWPT) {
          // Safety check to prevent a single DWPT exceeding its RAM limit. This
          // is super important since we can not address more than 2048 MB per DWPT
          setFlushPending(perThread);
        }
      }
      final DocumentsWriterPerThread flushingDWPT;
      if (fullFlush) {
        if (perThread.flushPending) {
          checkoutAndBlock(perThread);
          flushingDWPT = nextPendingFlush();
        } else {
          flushingDWPT = null;
        }
      } else {
       flushingDWPT = tryCheckoutForFlush(perThread);
      }
      return flushingDWPT;
    } finally {
      boolean stalled = updateStallState();
      assert assertNumDocsSinceStalled(stalled) && assertMemory();
    }
  }
  
  private boolean assertNumDocsSinceStalled(boolean stalled) {
    /*
     *  updates the number of documents "finished" while we are in a stalled state.
     *  this is important for asserting memory upper bounds since it corresponds 
     *  to the number of threads that are in-flight and crossed the stall control
     *  check before we actually stalled.
     *  see #assertMemory()
     */
    if (stalled) { 
      numDocsSinceStalled++;
    } else {
      numDocsSinceStalled = 0;
    }
    return true;
  }

  synchronized void doAfterFlush(DocumentsWriterPerThread dwpt) {
    assert flushingWriters.containsKey(dwpt);
    try {
      Long bytes = flushingWriters.remove(dwpt);
      flushBytes -= bytes.longValue();
      perThreadPool.recycle(dwpt);
      assert assertMemory();
    } finally {
      try {
        updateStallState();
      } finally {
        notifyAll();
      }
    }
  }
  
  private boolean updateStallState() {
    
    assert Thread.holdsLock(this);
    final long limit = stallLimitBytes();
    /*
     * we block indexing threads if net byte grows due to slow flushes
     * yet, for small ram buffers and large documents we can easily
     * reach the limit without any ongoing flushes. we need to ensure
     * that we don't stall/block if an ongoing or pending flush can
     * not free up enough memory to release the stall lock.
     */
    final boolean stall = (activeBytes + flushBytes) > limit &&
      activeBytes < limit &&
      !closed;
    stallControl.updateStalled(stall);
    return stall;
  }
  
  public synchronized void waitForFlush() {
    while (flushingWriters.size() != 0) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        throw new ThreadInterruptedException(e);
      }
    }
  }

  /**
   * Sets flush pending state on the given {@link ThreadState}. The
   * {@link ThreadState} must have indexed at least on Document and must not be
   * already pending.
   */
  public synchronized void setFlushPending(ThreadState perThread) {
    assert !perThread.flushPending;
    if (perThread.dwpt.getNumDocsInRAM() > 0) {
      perThread.flushPending = true; // write access synced
      final long bytes = perThread.bytesUsed;
      flushBytes += bytes;
      activeBytes -= bytes;
      numPending++; // write access synced
      assert assertMemory();
    } // don't assert on numDocs since we could hit an abort excp. while selecting that dwpt for flushing
    
  }
  
  synchronized void doOnAbort(ThreadState state) {
    try {
      if (state.flushPending) {
        flushBytes -= state.bytesUsed;
      } else {
        activeBytes -= state.bytesUsed;
      }
      assert assertMemory();
      // Take it out of the loop this DWPT is stale
      perThreadPool.reset(state);
    } finally {
      updateStallState();
    }
  }

  synchronized DocumentsWriterPerThread tryCheckoutForFlush(
      ThreadState perThread) {
   return perThread.flushPending ? internalTryCheckOutForFlush(perThread) : null;
  }
  
  private void checkoutAndBlock(ThreadState perThread) {
    perThread.lock();
    try {
      assert perThread.flushPending : "can not block non-pending threadstate";
      assert fullFlush : "can not block if fullFlush == false";
      final DocumentsWriterPerThread dwpt;
      final long bytes = perThread.bytesUsed;
      dwpt = perThreadPool.reset(perThread);
      numPending--;
      blockedFlushes.add(new BlockedFlush(dwpt, bytes));
    } finally {
      perThread.unlock();
    }
  }

  private DocumentsWriterPerThread internalTryCheckOutForFlush(ThreadState perThread) {
    assert Thread.holdsLock(this);
    assert perThread.flushPending;
    try {
      // We are pending so all memory is already moved to flushBytes
      if (perThread.tryLock()) {
        try {
          if (perThread.isInitialized()) {
            assert perThread.isHeldByCurrentThread();
            final DocumentsWriterPerThread dwpt;
            final long bytes = perThread.bytesUsed; // do that before
                                                         // replace!
            dwpt = perThreadPool.reset(perThread);
            assert !flushingWriters.containsKey(dwpt) : "DWPT is already flushing";
            // Record the flushing DWPT to reduce flushBytes in doAfterFlush
            flushingWriters.put(dwpt, Long.valueOf(bytes));
            numPending--; // write access synced
            return dwpt;
          }
        } finally {
          perThread.unlock();
        }
      }
      return null;
    } finally {
      updateStallState();
    }
  }

  @Override
  public String toString() {
    return "DocumentsWriterFlushControl [activeBytes=" + activeBytes
        + ", flushBytes=" + flushBytes + "]";
  }

  DocumentsWriterPerThread nextPendingFlush() {
    int numPending;
    boolean fullFlush;
    synchronized (this) {
      final DocumentsWriterPerThread poll;
      if ((poll = flushQueue.poll()) != null) {
        updateStallState();
        return poll;
      }
      fullFlush = this.fullFlush;
      numPending = this.numPending;
    }
    if (numPending > 0 && !fullFlush) { // don't check if we are doing a full flush
      final int limit = perThreadPool.getActiveThreadStateCount();
      for (int i = 0; i < limit && numPending > 0; i++) {
        final ThreadState next = perThreadPool.getThreadState(i);
        if (next.flushPending) {
          final DocumentsWriterPerThread dwpt = tryCheckoutForFlush(next);
          if (dwpt != null) {
            return dwpt;
          }
        }
      }
    }
    return null;
  }

  synchronized void setClosed() {
    // set by DW to signal that we should not release new DWPT after close
    this.closed = true;
  }

  /**
   * Returns an iterator that provides access to all currently active {@link ThreadState}s 
   */
  public Iterator<ThreadState> allActiveThreadStates() {
    return getPerThreadsIterator(perThreadPool.getActiveThreadStateCount());
  }
  
  private Iterator<ThreadState> getPerThreadsIterator(final int upto) {
    return new Iterator<ThreadState>() {
      int i = 0;

      @Override
      public boolean hasNext() {
        return i < upto;
      }

      @Override
      public ThreadState next() {
        return perThreadPool.getThreadState(i++);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove() not supported.");
      }
    };
  }

  

  synchronized void doOnDelete() {
    // pass null this is a global delete no update
    flushPolicy.onDelete(this, null);
  }

  /**
   * Returns the number of delete terms in the global pool
   */
  public int getNumGlobalTermDeletes() {
    return documentsWriter.deleteQueue.numGlobalTermDeletes() + bufferedUpdatesStream.numTerms();
  }
  
  public long getDeleteBytesUsed() {
    return documentsWriter.deleteQueue.ramBytesUsed() + bufferedUpdatesStream.ramBytesUsed();
  }

  @Override
  public long ramBytesUsed() {
    return getDeleteBytesUsed() + netBytes();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    // TODO: improve this?
    return Collections.emptyList();
  }

  synchronized int numFlushingDWPT() {
    return flushingWriters.size();
  }
  
  public boolean getAndResetApplyAllDeletes() {
    return flushDeletes.getAndSet(false);
  }

  public void setApplyAllDeletes() {
    flushDeletes.set(true);
  }
  
  int numActiveDWPT() {
    return this.perThreadPool.getActiveThreadStateCount();
  }
  
  ThreadState obtainAndLock() {
    final ThreadState perThread = perThreadPool.getAndLock(Thread
        .currentThread(), documentsWriter);
    boolean success = false;
    try {
      if (perThread.isInitialized()
          && perThread.dwpt.deleteQueue != documentsWriter.deleteQueue) {
        // There is a flush-all in process and this DWPT is
        // now stale -- enroll it for flush and try for
        // another DWPT:
        addFlushableState(perThread);
      }
      success = true;
      // simply return the ThreadState even in a flush all case sine we already hold the lock
      return perThread;
    } finally {
      if (!success) { // make sure we unlock if this fails
        perThreadPool.release(perThread);
      }
    }
  }
  
  void markForFullFlush() {
    final DocumentsWriterDeleteQueue flushingQueue;
    synchronized (this) {
      assert !fullFlush : "called DWFC#markForFullFlush() while full flush is still running";
      assert fullFlushBuffer.isEmpty() : "full flush buffer should be empty: "+ fullFlushBuffer;
      fullFlush = true;
      flushingQueue = documentsWriter.deleteQueue;
      // Set a new delete queue - all subsequent DWPT will use this queue until
      // we do another full flush
      DocumentsWriterDeleteQueue newQueue = new DocumentsWriterDeleteQueue(flushingQueue.generation+1);
      documentsWriter.deleteQueue = newQueue;
    }
    final int limit = perThreadPool.getActiveThreadStateCount();
    for (int i = 0; i < limit; i++) {
      final ThreadState next = perThreadPool.getThreadState(i);
      next.lock();
      try {
        if (!next.isInitialized()) {
          continue; 
        }
        assert next.dwpt.deleteQueue == flushingQueue
            || next.dwpt.deleteQueue == documentsWriter.deleteQueue : " flushingQueue: "
            + flushingQueue
            + " currentqueue: "
            + documentsWriter.deleteQueue
            + " perThread queue: "
            + next.dwpt.deleteQueue
            + " numDocsInRam: " + next.dwpt.getNumDocsInRAM();
        if (next.dwpt.deleteQueue != flushingQueue) {
          // this one is already a new DWPT
          continue;
        }
        addFlushableState(next);
      } finally {
        next.unlock();
      }
    }
    synchronized (this) {
      /* make sure we move all DWPT that are where concurrently marked as
       * pending and moved to blocked are moved over to the flushQueue. There is
       * a chance that this happens since we marking DWPT for full flush without
       * blocking indexing.*/
      pruneBlockedQueue(flushingQueue);   
      assert assertBlockedFlushes(documentsWriter.deleteQueue);
      flushQueue.addAll(fullFlushBuffer);
      fullFlushBuffer.clear();
      updateStallState();
    }
    assert assertActiveDeleteQueue(documentsWriter.deleteQueue);
  }
  
  private boolean assertActiveDeleteQueue(DocumentsWriterDeleteQueue queue) {
    final int limit = perThreadPool.getActiveThreadStateCount();
    for (int i = 0; i < limit; i++) {
      final ThreadState next = perThreadPool.getThreadState(i);
      next.lock();
      try {
        assert !next.isInitialized() || next.dwpt.deleteQueue == queue : "isInitialized: " + next.isInitialized() + " numDocs: " + (next.isInitialized() ? next.dwpt.getNumDocsInRAM() : 0) ;
      } finally {
        next.unlock();
      }
    }
    return true;
  }

  private final List<DocumentsWriterPerThread> fullFlushBuffer = new ArrayList<>();

  void addFlushableState(ThreadState perThread) {
    if (infoStream.isEnabled("DWFC")) {
      infoStream.message("DWFC", "addFlushableState " + perThread.dwpt);
    }
    final DocumentsWriterPerThread dwpt = perThread.dwpt;
    assert perThread.isHeldByCurrentThread();
    assert perThread.isInitialized();
    assert fullFlush;
    assert dwpt.deleteQueue != documentsWriter.deleteQueue;
    if (dwpt.getNumDocsInRAM() > 0) {
      synchronized(this) {
        if (!perThread.flushPending) {
          setFlushPending(perThread);
        }
        final DocumentsWriterPerThread flushingDWPT = internalTryCheckOutForFlush(perThread);
        assert flushingDWPT != null : "DWPT must never be null here since we hold the lock and it holds documents";
        assert dwpt == flushingDWPT : "flushControl returned different DWPT";
        fullFlushBuffer.add(flushingDWPT);
      }
    } else {
      perThreadPool.reset(perThread); // make this state inactive
    }
  }
  
  /**
   * Prunes the blockedQueue by removing all DWPT that are associated with the given flush queue. 
   */
  private void pruneBlockedQueue(final DocumentsWriterDeleteQueue flushingQueue) {
    Iterator<BlockedFlush> iterator = blockedFlushes.iterator();
    while (iterator.hasNext()) {
      BlockedFlush blockedFlush = iterator.next();
      if (blockedFlush.dwpt.deleteQueue == flushingQueue) {
        iterator.remove();
        assert !flushingWriters.containsKey(blockedFlush.dwpt) : "DWPT is already flushing";
        // Record the flushing DWPT to reduce flushBytes in doAfterFlush
        flushingWriters.put(blockedFlush.dwpt, Long.valueOf(blockedFlush.bytes));
        // don't decr pending here - it's already done when DWPT is blocked
        flushQueue.add(blockedFlush.dwpt);
      }
    }
  }
  
  synchronized void finishFullFlush() {
    assert fullFlush;
    assert flushQueue.isEmpty();
    assert flushingWriters.isEmpty();
    try {
      if (!blockedFlushes.isEmpty()) {
        assert assertBlockedFlushes(documentsWriter.deleteQueue);
        pruneBlockedQueue(documentsWriter.deleteQueue);
        assert blockedFlushes.isEmpty();
      }
    } finally {
      fullFlush = false;
      updateStallState();
    }
  }
  
  boolean assertBlockedFlushes(DocumentsWriterDeleteQueue flushingQueue) {
    for (BlockedFlush blockedFlush : blockedFlushes) {
      assert blockedFlush.dwpt.deleteQueue == flushingQueue;
    }
    return true;
  }

  synchronized void abortFullFlushes() {
   try {
     abortPendingFlushes();
   } finally {
     fullFlush = false;
   }
  }
  
  synchronized void abortPendingFlushes() {
    try {
      for (DocumentsWriterPerThread dwpt : flushQueue) {
        try {
          documentsWriter.subtractFlushedNumDocs(dwpt.getNumDocsInRAM());
          dwpt.abort();
        } catch (Throwable ex) {
          // ignore - keep on aborting the flush queue
        } finally {
          doAfterFlush(dwpt);
        }
      }
      for (BlockedFlush blockedFlush : blockedFlushes) {
        try {
          flushingWriters
              .put(blockedFlush.dwpt, Long.valueOf(blockedFlush.bytes));
          documentsWriter.subtractFlushedNumDocs(blockedFlush.dwpt.getNumDocsInRAM());
          blockedFlush.dwpt.abort();
        } catch (Throwable ex) {
          // ignore - keep on aborting the blocked queue
        } finally {
          doAfterFlush(blockedFlush.dwpt);
        }
      }
    } finally {
      flushQueue.clear();
      blockedFlushes.clear();
      updateStallState();
    }
  }
  
  /**
   * Returns <code>true</code> if a full flush is currently running
   */
  synchronized boolean isFullFlush() {
    return fullFlush;
  }

  /**
   * Returns the number of flushes that are already checked out but not yet
   * actively flushing
   */
  synchronized int numQueuedFlushes() {
    return flushQueue.size();
  }

  /**
   * Returns the number of flushes that are checked out but not yet available
   * for flushing. This only applies during a full flush if a DWPT needs
   * flushing but must not be flushed until the full flush has finished.
   */
  synchronized int numBlockedFlushes() {
    return blockedFlushes.size();
  }
  
  private static class BlockedFlush {
    final DocumentsWriterPerThread dwpt;
    final long bytes;
    BlockedFlush(DocumentsWriterPerThread dwpt, long bytes) {
      super();
      this.dwpt = dwpt;
      this.bytes = bytes;
    }
  }

  /**
   * This method will block if too many DWPT are currently flushing and no
   * checked out DWPT are available
   */
  void waitIfStalled() {
    if (infoStream.isEnabled("DWFC")) {
      infoStream.message("DWFC",
          "waitIfStalled: numFlushesPending: " + flushQueue.size()
              + " netBytes: " + netBytes() + " flushBytes: " + flushBytes()
              + " fullFlush: " + fullFlush);
    }
    stallControl.waitIfStalled();
  }

  /**
   * Returns <code>true</code> iff stalled
   */
  boolean anyStalledThreads() {
    return stallControl.anyStalledThreads();
  }
  
  /**
   * Returns the {@link IndexWriter} {@link InfoStream}
   */
  public InfoStream getInfoStream() {
    return infoStream;
  }
  
  
}
