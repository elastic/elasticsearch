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
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.IndexSettings;

/**
 * This class generates sequences numbers and keeps track of the so-called "local checkpoint" which is the highest number for which all
 * previous sequence numbers have been processed (inclusive).
 */
public class LocalCheckpointTracker {

    /**
     * We keep a bit for each sequence number that is still pending. To optimize allocation, we do so in multiple arrays allocating them on
     * demand and cleaning up while completed. This setting controls the size of the arrays.
     */
    public static Setting<Integer> SETTINGS_BIT_ARRAYS_SIZE =
        Setting.intSetting("index.seq_no.checkpoint.bit_arrays_size", 1024, 4, Setting.Property.IndexScope);

    /**
     * A collection of bit arrays representing pending sequence numbers. Each sequence number is mapped to a bit array by dividing by the
     * bit set size.
     */
    final LongObjectHashMap<FixedBitSet> processedSeqNo = new LongObjectHashMap<>();

    /**
     * The size of each bit set representing processed sequence numbers.
     */
    private final int bitArraysSize;

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
     * @param indexSettings   the index settings
     * @param maxSeqNo        the last sequence number assigned, or {@link SequenceNumbers#NO_OPS_PERFORMED}
     * @param localCheckpoint the last known local checkpoint, or {@link SequenceNumbers#NO_OPS_PERFORMED}
     */
    public LocalCheckpointTracker(final IndexSettings indexSettings, final long maxSeqNo, final long localCheckpoint) {
        if (localCheckpoint < 0 && localCheckpoint != SequenceNumbers.NO_OPS_PERFORMED) {
            throw new IllegalArgumentException(
                "local checkpoint must be non-negative or [" + SequenceNumbers.NO_OPS_PERFORMED + "] "
                    + "but was [" + localCheckpoint + "]");
        }
        if (maxSeqNo < 0 && maxSeqNo != SequenceNumbers.NO_OPS_PERFORMED) {
            throw new IllegalArgumentException(
                "max seq. no. must be non-negative or [" + SequenceNumbers.NO_OPS_PERFORMED + "] but was [" + maxSeqNo + "]");
        }
        bitArraysSize = SETTINGS_BIT_ARRAYS_SIZE.get(indexSettings.getSettings());
        nextSeqNo = maxSeqNo == SequenceNumbers.NO_OPS_PERFORMED ? 0 : maxSeqNo + 1;
        checkpoint = localCheckpoint;
    }

    /**
     * Issue the next sequence number.
     *
     * @return the next assigned sequence number
     */
    synchronized long generateSeqNo() {
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
        final FixedBitSet bitArray = getBitArrayForSeqNo(seqNo);
        final int offset = seqNoToBitArrayOffset(seqNo);
        bitArray.set(offset);
        if (seqNo == checkpoint + 1) {
            updateCheckpoint();
        }
    }

    /**
     * Resets the checkpoint to the specified value.
     *
     * @param checkpoint the local checkpoint to reset this tracker to
     */
    synchronized void resetCheckpoint(final long checkpoint) {
        assert checkpoint != SequenceNumbers.UNASSIGNED_SEQ_NO;
        assert checkpoint <= this.checkpoint;
        processedSeqNo.clear();
        this.checkpoint = checkpoint;
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
    long getMaxSeqNo() {
        return nextSeqNo - 1;
    }


    /**
     * constructs a {@link SeqNoStats} object, using local state and the supplied global checkpoint
     *
     * @implNote this is needed to make sure the local checkpoint and max seq no are consistent
     */
    synchronized SeqNoStats getStats(final long globalCheckpoint) {
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
        while (checkpoint < seqNo) {
            // notified by updateCheckpoint
            this.wait();
        }
    }

    /**
     * Moves the checkpoint to the last consecutively processed sequence number. This method assumes that the sequence number following the
     * current checkpoint is processed.
     */
    @SuppressForbidden(reason = "Object#notifyAll")
    private void updateCheckpoint() {
        assert Thread.holdsLock(this);
        assert getBitArrayForSeqNo(checkpoint + 1).get(seqNoToBitArrayOffset(checkpoint + 1)) :
            "updateCheckpoint is called but the bit following the checkpoint is not set";
        try {
            // keep it simple for now, get the checkpoint one by one; in the future we can optimize and read words
            long bitArrayKey = getBitArrayKey(checkpoint);
            FixedBitSet current = processedSeqNo.get(bitArrayKey);
            if (current == null) {
                // the bit set corresponding to the checkpoint has already been removed, set ourselves up for the next bit set
                assert checkpoint % bitArraysSize == bitArraysSize - 1;
                current = processedSeqNo.get(++bitArrayKey);
            }
            do {
                checkpoint++;
                /*
                 * The checkpoint always falls in the current bit set or we have already cleaned it; if it falls on the last bit of the
                 * current bit set, we can clean it.
                 */
                if (checkpoint == lastSeqNoInBitArray(bitArrayKey)) {
                    assert current != null;
                    final FixedBitSet removed = processedSeqNo.remove(bitArrayKey);
                    assert removed == current;
                    current = processedSeqNo.get(++bitArrayKey);
                }
            } while (current != null && current.get(seqNoToBitArrayOffset(checkpoint + 1)));
        } finally {
            // notifies waiters in waitForOpsToComplete
            this.notifyAll();
        }
    }

    private long lastSeqNoInBitArray(final long bitArrayKey) {
        return (1 + bitArrayKey) * bitArraysSize - 1;
    }

    /**
     * Return the bit array for the provided sequence number, possibly allocating a new array if needed.
     *
     * @param seqNo the sequence number to obtain the bit array for
     * @return the bit array corresponding to the provided sequence number
     */
    private long getBitArrayKey(final long seqNo) {
        assert Thread.holdsLock(this);
        return seqNo / bitArraysSize;
    }

    private FixedBitSet getBitArrayForSeqNo(final long seqNo) {
        assert Thread.holdsLock(this);
        final long bitArrayKey = getBitArrayKey(seqNo);
        final int index = processedSeqNo.indexOf(bitArrayKey);
        if (processedSeqNo.indexExists(index) == false) {
            processedSeqNo.indexInsert(index, bitArrayKey, new FixedBitSet(bitArraysSize));
        }
        return processedSeqNo.indexGet(index);
    }

    /**
     * Obtain the position in the bit array corresponding to the provided sequence number. The bit array corresponding to the sequence
     * number can be obtained via {@link #getBitArrayForSeqNo(long)}.
     *
     * @param seqNo the sequence number to obtain the position for
     * @return the position in the bit array corresponding to the provided sequence number
     */
    private int seqNoToBitArrayOffset(final long seqNo) {
        assert Thread.holdsLock(this);
        return Math.toIntExact(seqNo % bitArraysSize);
    }

}
