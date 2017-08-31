/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.common.SuppressForbidden;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class generates sequences numbers and keeps track of the so-called "local checkpoint" which is the highest number for which all
 * previous sequence numbers have been processed (inclusive).
 */
public final class LocalCheckpointTracker {
    /**
     * All sequence numbers (in ascending order) that have already been marked as completed but that are above the current checkpoint
     * because some sequence number in between has not been marked as completed yet.
     */
    private final ConcurrentSkipListSet<Long> sequenceNumbersInUse = new ConcurrentSkipListSet<>();

    /**
     * The current local checkpoint, i.e., all sequence numbers no more than this number have been completed.
     */
    private final AtomicLong checkpoint;

    /**
     * The next available sequence number.
     */
    private final AtomicLong nextSeqNo;

    /**
     * This lock and its corresponding read and write locks are not necessary for correct operation but to assert that
     * {@link #resetCheckpoint(long)} is never called concurrently with {@link #markSeqNoAsCompleted(long)}.
     */
    private final ReadWriteLock assertionLock = new ReentrantReadWriteLock();

    private final Lock assertionReadLock = assertionLock.readLock();

    private final Lock assertionWriteLock = assertionLock.readLock();

    /**
     * Initialize the local checkpoint tracker. The {@code maxSeqNo} should be set to the last sequence number assigned, or
     * {@link SequenceNumbersService#NO_OPS_PERFORMED} and {@code localCheckpoint} should be set to the last known local checkpoint,
     * or {@link SequenceNumbersService#NO_OPS_PERFORMED}.
     *
     * @param maxSeqNo        the last sequence number assigned, or {@link SequenceNumbersService#NO_OPS_PERFORMED}
     * @param localCheckpoint the last known local checkpoint, or {@link SequenceNumbersService#NO_OPS_PERFORMED}
     */
    public LocalCheckpointTracker(final long maxSeqNo, final long localCheckpoint) {
        if (localCheckpoint < 0 && localCheckpoint != SequenceNumbersService.NO_OPS_PERFORMED) {
            throw new IllegalArgumentException(
                "local checkpoint must be non-negative or [" + SequenceNumbersService.NO_OPS_PERFORMED + "] "
                    + "but was [" + localCheckpoint + "]");
        }
        if (maxSeqNo < 0 && maxSeqNo != SequenceNumbersService.NO_OPS_PERFORMED) {
            throw new IllegalArgumentException(
                "max seq. no. must be non-negative or [" + SequenceNumbersService.NO_OPS_PERFORMED + "] but was [" + maxSeqNo + "]");
        }
        long initialNextSeqNo = maxSeqNo == SequenceNumbersService.NO_OPS_PERFORMED ? 0 : maxSeqNo + 1;
        nextSeqNo = new AtomicLong(initialNextSeqNo);
        checkpoint = new AtomicLong(localCheckpoint);
    }

    /**
     * Issue the next sequence number.
     *
     * @return the next assigned sequence number
     */
    long generateSeqNo() {
        return nextSeqNo.getAndIncrement();
    }

    /**
     * Marks the processing of the provided sequence number as completed as updates the checkpoint if possible.
     *
     * @param seqNo the sequence number to mark as completed
     */
    public void markSeqNoAsCompleted(final long seqNo) {
        boolean locked = false;
        assert (locked = assertionReadLock.tryLock()) : "#resetCheckpoint() must not be called concurrently with other operations";
        try {
            if (seqNo <= checkpoint.get()) {
                // this is possible during recovery where we might replay an operation that was also replicated
                return;
            }
            // make sure we track highest seen sequence number
            nextSeqNo.updateAndGet((current) -> seqNo >= current ? seqNo + 1 : current);

            if (seqNo == checkpoint.get() + 1) {
                // we can immediately advance without remembering this sequence number
                checkpoint.incrementAndGet();
                // now that we have advanced the checkpoint, we might be able to advance further
                updateCheckpoint(true);
            } else {
                sequenceNumbersInUse.add(seqNo);
                updateCheckpoint(false);
            }
        } finally {
            if (locked) {
                assertionReadLock.unlock();
            }
        }
    }

    /**
     * Resets the checkpoint to the specified value.
     *
     * This implementation assumes that no other operation is called concurrently.
     *
     * @param checkpoint the local checkpoint to reset this tracker to
     */
    void resetCheckpoint(final long checkpoint) {
        boolean locked = false;
        assert checkpoint != SequenceNumbersService.UNASSIGNED_SEQ_NO;
        assert checkpoint <= this.checkpoint.get();
        assert (locked = assertionWriteLock.tryLock()) : "#resetCheckpoint() must not be called concurrently with other operations";
        try {
            this.checkpoint.set(checkpoint);
            this.sequenceNumbersInUse.clear();
        } finally {
            if (locked) {
                assertionWriteLock.unlock();
            }
        }
    }

    /**
     * The current checkpoint which can be advanced by {@link #markSeqNoAsCompleted(long)}.
     *
     * @return the current checkpoint
     */
    public long getCheckpoint() {
        return checkpoint.get();
    }

    /**
     * The maximum sequence number issued so far.
     *
     * @return the maximum sequence number
     */
    long getMaxSeqNo() {
        return nextSeqNo.get() - 1;
    }


    /**
     * constructs a {@link SeqNoStats} object, using local state and the supplied global checkpoint
     *
     * @implNote this is needed to make sure the local checkpoint and max seq no are consistent
     */
    SeqNoStats getStats(final long globalCheckpoint) {
        return new SeqNoStats(getMaxSeqNo(), getCheckpoint(), globalCheckpoint);
    }

    /**
     * Waits for all operations up to the provided sequence number to complete.
     *
     * @param seqNo the sequence number that the checkpoint must advance to before this method returns
     * @throws InterruptedException if the thread was interrupted while blocking on the condition
     */
    @SuppressForbidden(reason = "Object#wait")
    synchronized void waitForOpsToComplete(final long seqNo) throws InterruptedException {
        while (checkpoint.get() < seqNo) {
            // notified by updateCheckpoint
            this.wait();
        }
    }

    /**
     * Moves the checkpoint to the last consecutively processed sequence number. This method assumes that the sequence number following the
     * current checkpoint is processed.
     *
     * @param forceNotification <code>true</code> iff waiting threads should be notified regardless whether the checkpoint has been
     *                          advanced by this method.
     */
    @SuppressForbidden(reason = "Object#notifyAll")
    private void updateCheckpoint(boolean forceNotification) {
        assert sequenceNumbersInUse.floor(checkpoint.get()) == null :
            "All marked sequence numbers must be greater than the checkpoint [" + checkpoint.get() + "]";
        boolean needsNotification = forceNotification;
        while (sequenceNumbersInUse.remove(checkpoint.get() + 1)) {
            checkpoint.incrementAndGet();
            needsNotification = true;
        }
        if (needsNotification) {
            synchronized (this) {
                // notifies waiters in waitForOpsToComplete
                this.notifyAll();
            }
        }
    }
}
