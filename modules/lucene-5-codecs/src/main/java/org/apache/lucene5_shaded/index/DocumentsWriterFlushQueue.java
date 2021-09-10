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
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene5_shaded.index.DocumentsWriterPerThread.FlushedSegment;

/**
 * @lucene.internal 
 */
class DocumentsWriterFlushQueue {
  private final Queue<FlushTicket> queue = new LinkedList<>();
  // we track tickets separately since count must be present even before the ticket is
  // constructed ie. queue.size would not reflect it.
  private final AtomicInteger ticketCount = new AtomicInteger();
  private final ReentrantLock purgeLock = new ReentrantLock();

  void addDeletes(DocumentsWriterDeleteQueue deleteQueue) throws IOException {
    synchronized (this) {
      incTickets();// first inc the ticket count - freeze opens
                   // a window for #anyChanges to fail
      boolean success = false;
      try {
        queue.add(new GlobalDeletesTicket(deleteQueue.freezeGlobalBuffer(null)));
        success = true;
      } finally {
        if (!success) {
          decTickets();
        }
      }
    }
  }
  
  private void incTickets() {
    int numTickets = ticketCount.incrementAndGet();
    assert numTickets > 0;
  }
  
  private void decTickets() {
    int numTickets = ticketCount.decrementAndGet();
    assert numTickets >= 0;
  }

  synchronized SegmentFlushTicket addFlushTicket(DocumentsWriterPerThread dwpt) {
    // Each flush is assigned a ticket in the order they acquire the ticketQueue
    // lock
    incTickets();
    boolean success = false;
    try {
      // prepare flush freezes the global deletes - do in synced block!
      final SegmentFlushTicket ticket = new SegmentFlushTicket(dwpt.prepareFlush());
      queue.add(ticket);
      success = true;
      return ticket;
    } finally {
      if (!success) {
        decTickets();
      }
    }
  }
  
  synchronized void addSegment(SegmentFlushTicket ticket, FlushedSegment segment) {
    // the actual flush is done asynchronously and once done the FlushedSegment
    // is passed to the flush ticket
    ticket.setSegment(segment);
  }

  synchronized void markTicketFailed(SegmentFlushTicket ticket) {
    // to free the queue we mark tickets as failed just to clean up the queue.
    ticket.setFailed();
  }

  boolean hasTickets() {
    assert ticketCount.get() >= 0 : "ticketCount should be >= 0 but was: " + ticketCount.get();
    return ticketCount.get() != 0;
  }

  private int innerPurge(IndexWriter writer) throws IOException {
    assert purgeLock.isHeldByCurrentThread();
    int numPurged = 0;
    while (true) {
      final FlushTicket head;
      final boolean canPublish;
      synchronized (this) {
        head = queue.peek();
        canPublish = head != null && head.canPublish(); // do this synced 
      }
      if (canPublish) {
        numPurged++;
        try {
          /*
           * if we block on publish -> lock IW -> lock BufferedDeletes we don't block
           * concurrent segment flushes just because they want to append to the queue.
           * the downside is that we need to force a purge on fullFlush since ther could
           * be a ticket still in the queue. 
           */
          head.publish(writer);
          
        } finally {
          synchronized (this) {
            // finally remove the published ticket from the queue
            final FlushTicket poll = queue.poll();
            ticketCount.decrementAndGet();
            assert poll == head;
          }
        }
      } else {
        break;
      }
    }
    return numPurged;
  }

  int forcePurge(IndexWriter writer) throws IOException {
    assert !Thread.holdsLock(this);
    assert !Thread.holdsLock(writer);
    purgeLock.lock();
    try {
      return innerPurge(writer);
    } finally {
      purgeLock.unlock();
    }
  }

  int tryPurge(IndexWriter writer) throws IOException {
    assert !Thread.holdsLock(this);
    assert !Thread.holdsLock(writer);
    if (purgeLock.tryLock()) {
      try {
        return innerPurge(writer);
      } finally {
        purgeLock.unlock();
      }
    }
    return 0;
  }

  public int getTicketCount() {
    return ticketCount.get();
  }

  synchronized void clear() {
    queue.clear();
    ticketCount.set(0);
  }

  static abstract class FlushTicket {
    protected FrozenBufferedUpdates frozenUpdates;
    protected boolean published = false;

    protected FlushTicket(FrozenBufferedUpdates frozenUpdates) {
      assert frozenUpdates != null;
      this.frozenUpdates = frozenUpdates;
    }

    protected abstract void publish(IndexWriter writer) throws IOException;
    protected abstract boolean canPublish();
    
    /**
     * Publishes the flushed segment, segment private deletes (if any) and its
     * associated global delete (if present) to IndexWriter.  The actual
     * publishing operation is synced on {@code IW -> BDS} so that the {@link SegmentInfo}'s
     * delete generation is always GlobalPacket_deleteGeneration + 1
     */
    protected final void publishFlushedSegment(IndexWriter indexWriter, FlushedSegment newSegment, FrozenBufferedUpdates globalPacket)
        throws IOException {
      assert newSegment != null;
      assert newSegment.segmentInfo != null;
      final FrozenBufferedUpdates segmentUpdates = newSegment.segmentUpdates;
      //System.out.println("FLUSH: " + newSegment.segmentInfo.info.name);
      if (indexWriter.infoStream.isEnabled("DW")) {
          indexWriter.infoStream.message("DW", "publishFlushedSegment seg-private updates=" + segmentUpdates);  
      }
      
      if (segmentUpdates != null && indexWriter.infoStream.isEnabled("DW")) {
          indexWriter.infoStream.message("DW", "flush: push buffered seg private updates: " + segmentUpdates);
      }
      // now publish!
      indexWriter.publishFlushedSegment(newSegment.segmentInfo, segmentUpdates, globalPacket);
    }
    
    protected final void finishFlush(IndexWriter indexWriter, FlushedSegment newSegment, FrozenBufferedUpdates bufferedUpdates)
            throws IOException {
        // Finish the flushed segment and publish it to IndexWriter
        if (newSegment == null) {
          assert bufferedUpdates != null;
          if (bufferedUpdates != null && bufferedUpdates.any()) {
            indexWriter.publishFrozenUpdates(bufferedUpdates);
            if (indexWriter.infoStream.isEnabled("DW")) {
                indexWriter.infoStream.message("DW", "flush: push buffered updates: " + bufferedUpdates);
            }
          }
        } else {
            publishFlushedSegment(indexWriter, newSegment, bufferedUpdates);  
        }
      }
  }
  
  static final class GlobalDeletesTicket extends FlushTicket {

    protected GlobalDeletesTicket(FrozenBufferedUpdates frozenUpdates) {
      super(frozenUpdates);
    }
    @Override
    protected void publish(IndexWriter writer) throws IOException {
      assert !published : "ticket was already publised - can not publish twice";
      published = true;
      // it's a global ticket - no segment to publish
      finishFlush(writer, null, frozenUpdates);
    }

    @Override
    protected boolean canPublish() {
      return true;
    }
  }

  static final class SegmentFlushTicket extends FlushTicket {
    private FlushedSegment segment;
    private boolean failed = false;
    
    protected SegmentFlushTicket(FrozenBufferedUpdates frozenDeletes) {
      super(frozenDeletes);
    }
    
    @Override
    protected void publish(IndexWriter writer) throws IOException {
      assert !published : "ticket was already publised - can not publish twice";
      published = true;
      finishFlush(writer, segment, frozenUpdates);
    }
    
    protected void setSegment(FlushedSegment segment) {
      assert !failed;
      this.segment = segment;
    }
    
    protected void setFailed() {
      assert segment == null;
      failed = true;
    }

    @Override
    protected boolean canPublish() {
      return segment != null || failed;
    }
  }
}