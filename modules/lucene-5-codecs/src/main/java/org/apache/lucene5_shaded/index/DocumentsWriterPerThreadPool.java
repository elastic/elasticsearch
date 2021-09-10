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

import org.apache.lucene5_shaded.util.ThreadInterruptedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@link DocumentsWriterPerThreadPool} controls {@link ThreadState} instances
 * and their thread assignments during indexing. Each {@link ThreadState} holds
 * a reference to a {@link DocumentsWriterPerThread} that is once a
 * {@link ThreadState} is obtained from the pool exclusively used for indexing a
 * single document by the obtaining thread. Each indexing thread must obtain
 * such a {@link ThreadState} to make progress. Depending on the
 * {@link DocumentsWriterPerThreadPool} implementation {@link ThreadState}
 * assignments might differ from document to document.
 * <p>
 * Once a {@link DocumentsWriterPerThread} is selected for flush the thread pool
 * is reusing the flushing {@link DocumentsWriterPerThread}s ThreadState with a
 * new {@link DocumentsWriterPerThread} instance.
 * </p>
 */
final class DocumentsWriterPerThreadPool {
  
  /**
   * {@link ThreadState} references and guards a
   * {@link DocumentsWriterPerThread} instance that is used during indexing to
   * build a in-memory index segment. {@link ThreadState} also holds all flush
   * related per-thread data controlled by {@link DocumentsWriterFlushControl}.
   * <p>
   * A {@link ThreadState}, its methods and members should only accessed by one
   * thread a time. Users must acquire the lock via {@link ThreadState#lock()}
   * and release the lock in a finally block via {@link ThreadState#unlock()}
   * before accessing the state.
   */
  @SuppressWarnings("serial")
  final static class ThreadState extends ReentrantLock {
    DocumentsWriterPerThread dwpt;
    // TODO this should really be part of DocumentsWriterFlushControl
    // write access guarded by DocumentsWriterFlushControl
    volatile boolean flushPending = false;
    // TODO this should really be part of DocumentsWriterFlushControl
    // write access guarded by DocumentsWriterFlushControl
    long bytesUsed = 0;

    ThreadState(DocumentsWriterPerThread dpwt) {
      this.dwpt = dpwt;
    }
    
    private void reset() {
      assert this.isHeldByCurrentThread();
      this.dwpt = null;
      this.bytesUsed = 0;
      this.flushPending = false;
    }
    
    boolean isInitialized() {
      assert this.isHeldByCurrentThread();
      return dwpt != null;
    }
    
    /**
     * Returns the number of currently active bytes in this ThreadState's
     * {@link DocumentsWriterPerThread}
     */
    public long getBytesUsedPerThread() {
      assert this.isHeldByCurrentThread();
      // public for FlushPolicy
      return bytesUsed;
    }
    
    /**
     * Returns this {@link ThreadState}s {@link DocumentsWriterPerThread}
     */
    public DocumentsWriterPerThread getDocumentsWriterPerThread() {
      assert this.isHeldByCurrentThread();
      // public for FlushPolicy
      return dwpt;
    }
    
    /**
     * Returns <code>true</code> iff this {@link ThreadState} is marked as flush
     * pending otherwise <code>false</code>
     */
    public boolean isFlushPending() {
      return flushPending;
    }
  }

  private final List<ThreadState> threadStates = new ArrayList<>();

  private final List<ThreadState> freeList = new ArrayList<>();

  private boolean aborted;

  /**
   * Returns the active number of {@link ThreadState} instances.
   */
  synchronized int getActiveThreadStateCount() {
    return threadStates.size();
  }

  synchronized void setAbort() {
    aborted = true;
  }

  synchronized void clearAbort() {
    aborted = false;
    notifyAll();
  }

  /**
   * Returns a new {@link ThreadState} iff any new state is available otherwise
   * <code>null</code>.
   * <p>
   * NOTE: the returned {@link ThreadState} is already locked iff non-
   * <code>null</code>.
   * 
   * @return a new {@link ThreadState} iff any new state is available otherwise
   *         <code>null</code>
   */
  private synchronized ThreadState newThreadState() {
    while (aborted) {
      try {
        wait();
      } catch (InterruptedException ie) {
        throw new ThreadInterruptedException(ie);        
      }
    }
    ThreadState threadState = new ThreadState(null);
    threadState.lock(); // lock so nobody else will get this ThreadState
    threadStates.add(threadState);
    return threadState;
  }

  DocumentsWriterPerThread reset(ThreadState threadState) {
    assert threadState.isHeldByCurrentThread();
    final DocumentsWriterPerThread dwpt = threadState.dwpt;
    threadState.reset();
    return dwpt;
  }
  
  void recycle(DocumentsWriterPerThread dwpt) {
    // don't recycle DWPT by default
  }

  /** This method is used by DocumentsWriter/FlushControl to obtain a ThreadState to do an indexing operation (add/updateDocument). */
  ThreadState getAndLock(Thread requestingThread, DocumentsWriter documentsWriter) {
    ThreadState threadState = null;
    synchronized (this) {
      if (freeList.isEmpty()) {
        // ThreadState is already locked before return by this method:
        return newThreadState();
      } else {
        // Important that we are LIFO here! This way if number of concurrent indexing threads was once high, but has now reduced, we only use a
        // limited number of thread states:
        threadState = freeList.remove(freeList.size()-1);

        if (threadState.dwpt == null) {
          // This thread-state is not initialized, e.g. it
          // was just flushed. See if we can instead find
          // another free thread state that already has docs
          // indexed. This way if incoming thread concurrency
          // has decreased, we don't leave docs
          // indefinitely buffered, tying up RAM.  This
          // will instead get those thread states flushed,
          // freeing up RAM for larger segment flushes:
          for(int i=0;i<freeList.size();i++) {
            ThreadState ts = freeList.get(i);
            if (ts.dwpt != null) {
              // Use this one instead, and swap it with
              // the un-initialized one:
              freeList.set(i, threadState);
              threadState = ts;
              break;
            }
          }
        }
      }
    }

    // This could take time, e.g. if the threadState is [briefly] checked for flushing:
    threadState.lock();

    return threadState;
  }

  void release(ThreadState state) {
    state.unlock();
    synchronized (this) {
      freeList.add(state);
      // In case any thread is waiting, wake one of them up since we just released a thread state; notify() should be sufficient but we do
      // notifyAll defensively:
      notifyAll();
    }
  }
  
  /**
   * Returns the <i>i</i>th active {@link ThreadState} where <i>i</i> is the
   * given ord.
   * 
   * @param ord
   *          the ordinal of the {@link ThreadState}
   * @return the <i>i</i>th active {@link ThreadState} where <i>i</i> is the
   *         given ord.
   */
  synchronized ThreadState getThreadState(int ord) {
    return threadStates.get(ord);
  }

  synchronized int getMaxThreadStates() {
    return threadStates.size();
  }

  /**
   * Returns the ThreadState with the minimum estimated number of threads
   * waiting to acquire its lock or <code>null</code> if no {@link ThreadState}
   * is yet visible to the calling thread.
   */
  ThreadState minContendedThreadState() {
    ThreadState minThreadState = null;
    for (ThreadState state : threadStates) {
      if (minThreadState == null || state.getQueueLength() < minThreadState.getQueueLength()) {
        minThreadState = state;
      }
    }
    return minThreadState;
  }
}
