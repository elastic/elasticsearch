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

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.IndexSettings;

import java.util.LinkedList;

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
     * An ordered list of bit arrays representing pending sequence numbers. The list is "anchored" in {@link #firstProcessedSeqNo} which
     * marks the sequence number the fist bit in the first array corresponds to.
     */
    final LinkedList<FixedBitSet> processedSeqNo = new LinkedList<>();

    /**
     * The size of each bit set representing processed sequence numbers.
     */
    private final int bitArraysSize;

    /**
     * The sequence number that the first bit in the first array corresponds to.
     */
    long firstProcessedSeqNo;

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
     * {@link SequenceNumbersService#NO_OPS_PERFORMED} and {@code localCheckpoint} should be set to the last known local checkpoint,
     * or {@link SequenceNumbersService#NO_OPS_PERFORMED}.
     *
     * @param indexSettings   the index settings
     * @param maxSeqNo        the last sequence number assigned, or {@link SequenceNumbersService#NO_OPS_PERFORMED}
     * @param localCheckpoint the last known local checkpoint, or {@link SequenceNumbersService#NO_OPS_PERFORMED}
     */
    public LocalCheckpointTracker(final IndexSettings indexSettings, final long maxSeqNo, final long localCheckpoint) {
        if (localCheckpoint < 0 && localCheckpoint != SequenceNumbersService.NO_OPS_PERFORMED) {
            throw new IllegalArgumentException(
                "local checkpoint must be non-negative or [" + SequenceNumbersService.NO_OPS_PERFORMED + "] "
                    + "but was [" + localCheckpoint + "]");
        }
        if (maxSeqNo < 0 && maxSeqNo != SequenceNumbersService.NO_OPS_PERFORMED) {
            throw new IllegalArgumentException(
                "max seq. no. must be non-negative or [" + SequenceNumbersService.NO_OPS_PERFORMED + "] but was [" + maxSeqNo + "]");
        }
        bitArraysSize = SETTINGS_BIT_ARRAYS_SIZE.get(indexSettings.getSettings());
        firstProcessedSeqNo = localCheckpoint == SequenceNumbersService.NO_OPS_PERFORMED ? 0 : localCheckpoint + 1;
        nextSeqNo = maxSeqNo == SequenceNumbersService.NO_OPS_PERFORMED ? 0 : maxSeqNo + 1;
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
        final FixedBitSet bitSet = getBitSetForSeqNo(seqNo);
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
        assert checkpoint < firstProcessedSeqNo + bitArraysSize - 1 :
            "checkpoint should be below the end of the first bit set (o.w. current bit set is completed and shouldn't be there)";
        assert getBitSetForSeqNo(checkpoint + 1) == processedSeqNo.getFirst() :
            "checkpoint + 1 doesn't point to the first bit set (o.w. current bit set is completed and shouldn't be there)";
        assert getBitSetForSeqNo(checkpoint + 1).get(seqNoToBitSetOffset(checkpoint + 1)) :
            "updateCheckpoint is called but the bit following the checkpoint is not set";
        try {
            // keep it simple for now, get the checkpoint one by one; in the future we can optimize and read words
            FixedBitSet current = processedSeqNo.getFirst();
            do {
                checkpoint++;
                // the checkpoint always falls in the first bit set or just before. If it falls
                // on the last bit of the current bit set, we can clean it.
                if (checkpoint == firstProcessedSeqNo + bitArraysSize - 1) {
                    processedSeqNo.removeFirst();
                    firstProcessedSeqNo += bitArraysSize;
                    assert checkpoint - firstProcessedSeqNo < bitArraysSize;
                    current = processedSeqNo.peekFirst();
                }
            } while (current != null && current.get(seqNoToBitSetOffset(checkpoint + 1)));
        } finally {
            // notifies waiters in waitForOpsToComplete
            this.notifyAll();
        }
    }

    /**
     * Return the bit array for the provided sequence number, possibly allocating a new array if needed.
     *
     * @param seqNo the sequence number to obtain the bit array for
     * @return the bit array corresponding to the provided sequence number
     */
    private FixedBitSet getBitSetForSeqNo(final long seqNo) {
        assert Thread.holdsLock(this);
        assert seqNo >= firstProcessedSeqNo : "seqNo: " + seqNo + " firstProcessedSeqNo: " + firstProcessedSeqNo;
        final long bitSetOffset = (seqNo - firstProcessedSeqNo) / bitArraysSize;
        if (bitSetOffset > Integer.MAX_VALUE) {
            throw new IndexOutOfBoundsException(
                "sequence number too high; got [" + seqNo + "], firstProcessedSeqNo [" + firstProcessedSeqNo + "]");
        }
        while (bitSetOffset >= processedSeqNo.size()) {
            processedSeqNo.add(new FixedBitSet(bitArraysSize));
        }
        return processedSeqNo.get((int) bitSetOffset);
    }

    /**
     * Obtain the position in the bit array corresponding to the provided sequence number. The bit array corresponding to the sequence
     * number can be obtained via {@link #getBitSetForSeqNo(long)}.
     *
     * @param seqNo the sequence number to obtain the position for
     * @return the position in the bit array corresponding to the provided sequence number
     */
    private int seqNoToBitSetOffset(final long seqNo) {
        assert Thread.holdsLock(this);
        assert seqNo >= firstProcessedSeqNo;
        return ((int) (seqNo - firstProcessedSeqNo)) % bitArraysSize;
    }

}
