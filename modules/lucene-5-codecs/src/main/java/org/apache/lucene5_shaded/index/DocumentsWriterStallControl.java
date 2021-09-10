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

import java.util.IdentityHashMap;
import java.util.Map;

import org.apache.lucene5_shaded.index.DocumentsWriterPerThreadPool.ThreadState;
import org.apache.lucene5_shaded.util.InfoStream;
import org.apache.lucene5_shaded.util.ThreadInterruptedException;

/**
 * Controls the health status of a {@link DocumentsWriter} sessions. This class
 * used to block incoming indexing threads if flushing significantly slower than
 * indexing to ensure the {@link DocumentsWriter}s healthiness. If flushing is
 * significantly slower than indexing the net memory used within an
 * {@link IndexWriter} session can increase very quickly and easily exceed the
 * JVM's available memory.
 * <p>
 * To prevent OOM Errors and ensure IndexWriter's stability this class blocks
 * incoming threads from indexing once 2 x number of available
 * {@link ThreadState}s in {@link DocumentsWriterPerThreadPool} is exceeded.
 * Once flushing catches up and the number of flushing DWPT is equal or lower
 * than the number of active {@link ThreadState}s threads are released and can
 * continue indexing.
 */
final class DocumentsWriterStallControl {
  
  private volatile boolean stalled;
  private int numWaiting; // only with assert
  private boolean wasStalled; // only with assert
  private final Map<Thread, Boolean> waiting = new IdentityHashMap<>(); // only with assert
  private final InfoStream infoStream;

  DocumentsWriterStallControl(LiveIndexWriterConfig iwc) {
    infoStream = iwc.getInfoStream();
  }
  
  /**
   * Update the stalled flag status. This method will set the stalled flag to
   * <code>true</code> iff the number of flushing
   * {@link DocumentsWriterPerThread} is greater than the number of active
   * {@link DocumentsWriterPerThread}. Otherwise it will reset the
   * {@link DocumentsWriterStallControl} to healthy and release all threads
   * waiting on {@link #waitIfStalled()}
   */
  synchronized void updateStalled(boolean stalled) {
    this.stalled = stalled;
    if (stalled) {
      wasStalled = true;
    }
    notifyAll();
  }
  
  /**
   * Blocks if documents writing is currently in a stalled state. 
   * 
   */
  void waitIfStalled() {
    if (stalled) {
      synchronized (this) {
        if (stalled) { // react on the first wakeup call!
          // don't loop here, higher level logic will re-stall!
          try {
            incWaiters();
            // Defensive, in case we have a concurrency bug that fails to .notify/All our thread:
            // just wait for up to 1 second here, and let caller re-stall if it's still needed:
            wait(1000);
            decrWaiters();
          } catch (InterruptedException e) {
            throw new ThreadInterruptedException(e);
          }
        }
      }
    }
  }
  
  boolean anyStalledThreads() {
    return stalled;
  }
  
  long stallStartNS;

  private void incWaiters() {
    stallStartNS = System.nanoTime();
    if (infoStream.isEnabled("DW") && numWaiting == 0) {
      infoStream.message("DW", "now stalling flushes");
    }
    numWaiting++;
    assert waiting.put(Thread.currentThread(), Boolean.TRUE) == null;
    assert numWaiting > 0;
  }
  
  private void decrWaiters() {
    numWaiting--;
    assert waiting.remove(Thread.currentThread()) != null;
    assert numWaiting >= 0;
    if (infoStream.isEnabled("DW") && numWaiting == 0) {
      long stallEndNS = System.nanoTime();
      infoStream.message("DW", "done stalling flushes for " + ((stallEndNS - stallStartNS)/1000000.0) + " ms");
    }
  }
  
  synchronized boolean hasBlocked() { // for tests
    return numWaiting > 0;
  }
  
  boolean isHealthy() { // for tests
    return !stalled; // volatile read!
  }
  
  synchronized boolean isThreadQueued(Thread t) { // for tests
    return waiting.containsKey(t);
  }

  synchronized boolean wasStalled() { // for tests
    return wasStalled;
  }
}
