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

import com.carrotsearch.hppc.LongObjectHashMap;
import org.elasticsearch.common.SuppressForbidden;

/**
 * This class generates sequences numbers and keeps track of the so-called "local checkpoint" which is the highest number for which all
 * previous sequence numbers have been processed (inclusive).
 */
public class LocalCheckpointTracker {

    /**
     * We keep a bit for each sequence number that is still pending. To optimize allocation, we do so in multiple sets allocating them on
     * demand and cleaning up while completed. This constant controls the size of the sets.
     */
    static final short BIT_SET_SIZE = 1024;

    /**
     * A collection of bit sets representing pending sequence numbers. Each sequence number is mapped to a bit set by dividing by the
     * bit set size.
     */
    final LongObjectHashMap<CountedBitSet> processedSeqNo = new LongObjectHashMap<>();

    /**
     * The current local checkpoint, i.e., all sequence numbers no more than this number have been completed.
     */
    volatile long checkpoint;

    /**
     * The next available sequence number.
     */
    private volatile long nextSeqNo;

    /**
     * Initialize the local checkpoint service. The {@code maxSeqNo} should be set to the last sequence number assigned, or
     * {@link SequenceNumbers#NO_OPS_PERFORMED} and {@code localCheckpoint} should be set to the last known local checkpoint,
     * or {@link SequenceNumbers#NO_OPS_PERFORMED}.
     *
     * @param maxSeqNo        the last sequence number assigned, or {@link SequenceNumbers#NO_OPS_PERFORMED}
     * @param localCheckpoint the last known local checkpoint, or {@link SequenceNumbers#NO_OPS_PERFORMED}
     */
    public LocalCheckpointTracker(final long maxSeqNo, final long localCheckpoint) {
        if (localCheckpoint < 0 && localCheckpoint != SequenceNumbers.NO_OPS_PERFORMED) {
            throw new IllegalArgumentException(
                "local checkpoint must be non-negative or [" + SequenceNumbers.NO_OPS_PERFORMED + "] "
                    + "but was [" + localCheckpoint + "]");
        }
        if (maxSeqNo < 0 && maxSeqNo != SequenceNumbers.NO_OPS_PERFORMED) {
            throw new IllegalArgumentException(
                "max seq. no. must be non-negative or [" + SequenceNumbers.NO_OPS_PERFORMED + "] but was [" + maxSeqNo + "]");
        }
        nextSeqNo = maxSeqNo == SequenceNumbers.NO_OPS_PERFORMED ? 0 : maxSeqNo + 1;
        checkpoint = localCheckpoint;
    }

    /**
     * Issue the next sequence number.
     *
     * @return the next assigned sequence number
     */
    public synchronized long generateSeqNo() {
        return nextSeqNo++;
    }

    /**
     * Marks the processing of the provided sequence number as completed as updates the checkpoint if possible.
     *
     * @param seqNo the sequence number to mark as completed
     */
    public synchronized void markSeqNoAsCompleted(final long seqNo) {
        // make sure we track highest seen sequence number
        if (seqNo >= nextSeqNo) {
            nextSeqNo = seqNo + 1;
        }
        if (seqNo <= checkpoint) {
            // this is possible during recovery where we might replay an operation that was also replicated
            return;
        }
        final CountedBitSet bitSet = getBitSetForSeqNo(seqNo);
        final int offset = seqNoToBitSetOffset(seqNo);
        bitSet.set(offset);
        if (seqNo == checkpoint + 1) {
            updateCheckpoint();
        }
    }

    /**
     * The current checkpoint which can be advanced by {@link #markSeqNoAsCompleted(long)}.
     *
     * @return the current checkpoint
     */
    public long getCheckpoint() {
        return checkpoint;
    }

    /**
     * The maximum sequence number issued so far.
     *
     * @return the maximum sequence number
     */
    public long getMaxSeqNo() {
        return nextSeqNo - 1;
    }


    /**
     * constructs a {@link SeqNoStats} object, using local state and the supplied global checkpoint
     *
     * This is needed to make sure the local checkpoint and max seq no are consistent
     */
    public synchronized SeqNoStats getStats(final long globalCheckpoint) {
        return new SeqNoStats(getMaxSeqNo(), getCheckpoint(), globalCheckpoint);
    }

    /**
     * Waits for all operations up to the provided sequence number to complete.
     *
     * @param seqNo the sequence number that the checkpoint must advance to before this method returns
     * @throws InterruptedException if the thread was interrupted while blocking on the condition
     */
    @SuppressForbidden(reason = "Object#wait")
    public synchronized void waitForOpsToComplete(final long seqNo) throws InterruptedException {
        while (checkpoint < seqNo) {
            // notified by updateCheckpoint
            this.wait();
        }
    }

    /**
     * Checks if the given sequence number was marked as completed in this tracker.
     */
    public boolean contains(final long seqNo) {
        assert seqNo >= 0 : "invalid seq_no=" + seqNo;
        if (seqNo >= nextSeqNo) {
            return false;
        }
        if (seqNo <= checkpoint) {
            return true;
        }
        final long bitSetKey = getBitSetKey(seqNo);
        final CountedBitSet bitSet;
        synchronized (this) {
            bitSet = processedSeqNo.get(bitSetKey);
        }
        return bitSet != null && bitSet.get(seqNoToBitSetOffset(seqNo));
    }

    /**
     * Moves the checkpoint to the last consecutively processed sequence number. This method assumes that the sequence number following the
     * current checkpoint is processed.
     */
    @SuppressForbidden(reason = "Object#notifyAll")
    private void updateCheckpoint() {
        assert Thread.holdsLock(this);
        assert getBitSetForSeqNo(checkpoint + 1).get(seqNoToBitSetOffset(checkpoint + 1)) :
            "updateCheckpoint is called but the bit following the checkpoint is not set";
        try {
            // keep it simple for now, get the checkpoint one by one; in the future we can optimize and read words
            long bitSetKey = getBitSetKey(checkpoint);
            CountedBitSet current = processedSeqNo.get(bitSetKey);
            if (current == null) {
                // the bit set corresponding to the checkpoint has already been removed, set ourselves up for the next bit set
                assert checkpoint % BIT_SET_SIZE == BIT_SET_SIZE - 1;
                current = processedSeqNo.get(++bitSetKey);
            }
            do {
                checkpoint++;
                /*
                 * The checkpoint always falls in the current bit set or we have already cleaned it; if it falls on the last bit of the
                 * current bit set, we can clean it.
                 */
                if (checkpoint == lastSeqNoInBitSet(bitSetKey)) {
                    assert current != null;
                    final CountedBitSet removed = processedSeqNo.remove(bitSetKey);
                    assert removed == current;
                    current = processedSeqNo.get(++bitSetKey);
                }
            } while (current != null && current.get(seqNoToBitSetOffset(checkpoint + 1)));
        } finally {
            // notifies waiters in waitForOpsToComplete
            this.notifyAll();
        }
    }

    private long lastSeqNoInBitSet(final long bitSetKey) {
        return (1 + bitSetKey) * BIT_SET_SIZE - 1;
    }

    /**
     * Return the bit set for the provided sequence number, possibly allocating a new set if needed.
     *
     * @param seqNo the sequence number to obtain the bit set for
     * @return the bit set corresponding to the provided sequence number
     */
    private long getBitSetKey(final long seqNo) {
        return seqNo / BIT_SET_SIZE;
    }

    private CountedBitSet getBitSetForSeqNo(final long seqNo) {
        assert Thread.holdsLock(this);
        final long bitSetKey = getBitSetKey(seqNo);
        final int index = processedSeqNo.indexOf(bitSetKey);
        final CountedBitSet bitSet;
        if (processedSeqNo.indexExists(index)) {
            bitSet = processedSeqNo.indexGet(index);
        } else {
            bitSet = new CountedBitSet(BIT_SET_SIZE);
            processedSeqNo.indexInsert(index, bitSetKey, bitSet);
        }
        return bitSet;
    }

    /**
     * Obtain the position in the bit set corresponding to the provided sequence number. The bit set corresponding to the sequence number
     * can be obtained via {@link #getBitSetForSeqNo(long)}.
     *
     * @param seqNo the sequence number to obtain the position for
     * @return the position in the bit set corresponding to the provided sequence number
     */
    private int seqNoToBitSetOffset(final long seqNo) {
        return Math.toIntExact(seqNo % BIT_SET_SIZE);
    }

}
