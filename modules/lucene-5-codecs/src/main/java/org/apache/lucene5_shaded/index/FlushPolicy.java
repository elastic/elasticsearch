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

import java.util.Iterator;

import org.apache.lucene5_shaded.index.DocumentsWriterPerThreadPool.ThreadState;
import org.apache.lucene5_shaded.store.Directory;
import org.apache.lucene5_shaded.util.InfoStream;

/**
 * {@link FlushPolicy} controls when segments are flushed from a RAM resident
 * internal data-structure to the {@link IndexWriter}s {@link Directory}.
 * <p>
 * Segments are traditionally flushed by:
 * <ul>
 * <li>RAM consumption - configured via
 * {@link IndexWriterConfig#setRAMBufferSizeMB(double)}</li>
 * <li>Number of RAM resident documents - configured via
 * {@link IndexWriterConfig#setMaxBufferedDocs(int)}</li>
 * </ul>
 * The policy also applies pending delete operations (by term and/or query),
 * given the threshold set in
 * {@link IndexWriterConfig#setMaxBufferedDeleteTerms(int)}.
 * <p>
 * {@link IndexWriter} consults the provided {@link FlushPolicy} to control the
 * flushing process. The policy is informed for each added or updated document
 * as well as for each delete term. Based on the {@link FlushPolicy}, the
 * information provided via {@link ThreadState} and
 * {@link DocumentsWriterFlushControl}, the {@link FlushPolicy} decides if a
 * {@link DocumentsWriterPerThread} needs flushing and mark it as flush-pending
 * via {@link DocumentsWriterFlushControl#setFlushPending}, or if deletes need
 * to be applied.
 * 
 * @see ThreadState
 * @see DocumentsWriterFlushControl
 * @see DocumentsWriterPerThread
 * @see IndexWriterConfig#setFlushPolicy(FlushPolicy)
 */
abstract class FlushPolicy {
  protected LiveIndexWriterConfig indexWriterConfig;
  protected InfoStream infoStream;

  /**
   * Called for each delete term. If this is a delete triggered due to an update
   * the given {@link ThreadState} is non-null.
   * <p>
   * Note: This method is called synchronized on the given
   * {@link DocumentsWriterFlushControl} and it is guaranteed that the calling
   * thread holds the lock on the given {@link ThreadState}
   */
  public abstract void onDelete(DocumentsWriterFlushControl control,
      ThreadState state);

  /**
   * Called for each document update on the given {@link ThreadState}'s
   * {@link DocumentsWriterPerThread}.
   * <p>
   * Note: This method is called  synchronized on the given
   * {@link DocumentsWriterFlushControl} and it is guaranteed that the calling
   * thread holds the lock on the given {@link ThreadState}
   */
  public void onUpdate(DocumentsWriterFlushControl control, ThreadState state) {
    onInsert(control, state);
    onDelete(control, state);
  }

  /**
   * Called for each document addition on the given {@link ThreadState}s
   * {@link DocumentsWriterPerThread}.
   * <p>
   * Note: This method is synchronized by the given
   * {@link DocumentsWriterFlushControl} and it is guaranteed that the calling
   * thread holds the lock on the given {@link ThreadState}
   */
  public abstract void onInsert(DocumentsWriterFlushControl control,
      ThreadState state);

  /**
   * Called by DocumentsWriter to initialize the FlushPolicy
   */
  protected synchronized void init(LiveIndexWriterConfig indexWriterConfig) {
    this.indexWriterConfig = indexWriterConfig;
    infoStream = indexWriterConfig.getInfoStream();
  }

  /**
   * Returns the current most RAM consuming non-pending {@link ThreadState} with
   * at least one indexed document.
   * <p>
   * This method will never return <code>null</code>
   */
  protected ThreadState findLargestNonPendingWriter(
      DocumentsWriterFlushControl control, ThreadState perThreadState) {
    assert perThreadState.dwpt.getNumDocsInRAM() > 0;
    long maxRamSoFar = perThreadState.bytesUsed;
    // the dwpt which needs to be flushed eventually
    ThreadState maxRamUsingThreadState = perThreadState;
    assert !perThreadState.flushPending : "DWPT should have flushed";
    Iterator<ThreadState> activePerThreadsIterator = control.allActiveThreadStates();
    int count = 0;
    while (activePerThreadsIterator.hasNext()) {
      ThreadState next = activePerThreadsIterator.next();
      if (!next.flushPending) {
        final long nextRam = next.bytesUsed;
        if (nextRam > 0 && next.dwpt.getNumDocsInRAM() > 0) {
          if (infoStream.isEnabled("FP")) {
            infoStream.message("FP", "thread state has " + nextRam + " bytes; docInRAM=" + next.dwpt.getNumDocsInRAM());
          }
          count++;
          if (nextRam > maxRamSoFar) {
            maxRamSoFar = nextRam;
            maxRamUsingThreadState = next;
          }
        }
      }
    }
    if (infoStream.isEnabled("FP")) {
      infoStream.message("FP", count + " in-use non-flushing threads states");
    }
    assert assertMessage("set largest ram consuming thread pending on lower watermark");
    return maxRamUsingThreadState;
  }
  
  private boolean assertMessage(String s) {
    if (infoStream.isEnabled("FP")) {
      infoStream.message("FP", s);
    }
    return true;
  }
}
