/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.seqno;

import com.carrotsearch.hppc.LongObjectHashMap;

import java.util.concurrent.atomic.AtomicLong;

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
     * A collection of bit sets representing processed sequence numbers. Each sequence number is mapped to a bit set by dividing by the
     * bit set size.
     */
    final LongObjectHashMap<CountedBitSet> processedSeqNo = new LongObjectHashMap<>();

    /**
     * A collection of bit sets representing durably persisted sequence numbers. Each sequence number is mapped to a bit set by dividing by
     * the bit set size.
     */
    final LongObjectHashMap<CountedBitSet> persistedSeqNo = new LongObjectHashMap<>();

    /**
     * The current local checkpoint, i.e., all sequence numbers no more than this number have been processed.
     */
    final AtomicLong processedCheckpoint = new AtomicLong();

    /**
     * The current persisted local checkpoint, i.e., all sequence numbers no more than this number have been durably persisted.
     */
    final AtomicLong persistedCheckpoint = new AtomicLong();

    /**
     * The next available sequence number.
     */
    final AtomicLong nextSeqNo = new AtomicLong();

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
        nextSeqNo.set(maxSeqNo + 1);
        processedCheckpoint.set(localCheckpoint);
        persistedCheckpoint.set(localCheckpoint);
    }

    /**
     * Issue the next sequence number.
     *
     * @return the next assigned sequence number
     */
    public long generateSeqNo() {
        return nextSeqNo.getAndIncrement();
    }

    /**
     * Marks the provided sequence number as seen and updates the max_seq_no if needed.
     */
    public void advanceMaxSeqNo(final long seqNo) {
        nextSeqNo.accumulateAndGet(seqNo + 1, Math::max);
    }

    /**
     * Marks the provided sequence number as processed and updates the processed checkpoint if possible.
     *
     * @param seqNo the sequence number to mark as processed
     */
    public synchronized void markSeqNoAsProcessed(final long seqNo) {
        markSeqNo(seqNo, processedCheckpoint, processedSeqNo);
    }

    /**
     * Marks the provided sequence number as persisted and updates the checkpoint if possible.
     *
     * @param seqNo the sequence number to mark as persisted
     */
    public synchronized void markSeqNoAsPersisted(final long seqNo) {
        markSeqNo(seqNo, persistedCheckpoint, persistedSeqNo);
    }

    private void markSeqNo(final long seqNo, final AtomicLong checkPoint, final LongObjectHashMap<CountedBitSet> bitSetMap) {
        assert Thread.holdsLock(this);
        // make sure we track highest seen sequence number
        advanceMaxSeqNo(seqNo);
        if (seqNo <= checkPoint.get()) {
            // this is possible during recovery where we might replay an operation that was also replicated
            return;
        }
        final CountedBitSet bitSet = getBitSetForSeqNo(bitSetMap, seqNo);
        final int offset = seqNoToBitSetOffset(seqNo);
        bitSet.set(offset);
        if (seqNo == checkPoint.get() + 1) {
            updateCheckpoint(checkPoint, bitSetMap);
        }
    }

    /**
     * The current checkpoint which can be advanced by {@link #markSeqNoAsProcessed(long)}.
     *
     * @return the current checkpoint
     */
    public long getProcessedCheckpoint() {
        return processedCheckpoint.get();
    }

    /**
     * The current persisted checkpoint which can be advanced by {@link #markSeqNoAsPersisted(long)}.
     *
     * @return the current persisted checkpoint
     */
    public long getPersistedCheckpoint() {
        return persistedCheckpoint.get();
    }

    /**
     * The maximum sequence number issued so far.
     *
     * @return the maximum sequence number
     */
    public long getMaxSeqNo() {
        return nextSeqNo.get() - 1;
    }


    /**
     * constructs a {@link SeqNoStats} object, using local state and the supplied global checkpoint
     *
     * This is needed to make sure the persisted local checkpoint and max seq no are consistent
     */
    public synchronized SeqNoStats getStats(final long globalCheckpoint) {
        return new SeqNoStats(getMaxSeqNo(), getPersistedCheckpoint(), globalCheckpoint);
    }

    /**
     * Checks if the given sequence number was marked as processed in this tracker.
     */
    public boolean hasProcessed(final long seqNo) {
        assert seqNo >= 0 : "invalid seq_no=" + seqNo;
        if (seqNo >= nextSeqNo.get()) {
            return false;
        }
        if (seqNo <= processedCheckpoint.get()) {
            return true;
        }
        final long bitSetKey = getBitSetKey(seqNo);
        final int bitSetOffset = seqNoToBitSetOffset(seqNo);
        synchronized (this) {
            // check again under lock
            if (seqNo <= processedCheckpoint.get()) {
                return true;
            }
            final CountedBitSet bitSet = processedSeqNo.get(bitSetKey);
            return bitSet != null && bitSet.get(bitSetOffset);
        }
    }

    /**
     * Moves the checkpoint to the last consecutively processed sequence number. This method assumes that the sequence number
     * following the current checkpoint is processed.
     */
    private void updateCheckpoint(AtomicLong checkPoint, LongObjectHashMap<CountedBitSet> bitSetMap) {
        assert Thread.holdsLock(this);
        assert getBitSetForSeqNo(bitSetMap, checkPoint.get() + 1).get(seqNoToBitSetOffset(checkPoint.get() + 1)) :
                "updateCheckpoint is called but the bit following the checkpoint is not set";
        // keep it simple for now, get the checkpoint one by one; in the future we can optimize and read words
        long bitSetKey = getBitSetKey(checkPoint.get());
        CountedBitSet current = bitSetMap.get(bitSetKey);
        if (current == null) {
            // the bit set corresponding to the checkpoint has already been removed, set ourselves up for the next bit set
            assert checkPoint.get() % BIT_SET_SIZE == BIT_SET_SIZE - 1;
            current = bitSetMap.get(++bitSetKey);
        }
        do {
            checkPoint.incrementAndGet();
            /*
             * The checkpoint always falls in the current bit set or we have already cleaned it; if it falls on the last bit of the
             * current bit set, we can clean it.
             */
            if (checkPoint.get() == lastSeqNoInBitSet(bitSetKey)) {
                assert current != null;
                final CountedBitSet removed = bitSetMap.remove(bitSetKey);
                assert removed == current;
                current = bitSetMap.get(++bitSetKey);
            }
        } while (current != null && current.get(seqNoToBitSetOffset(checkPoint.get() + 1)));
    }

    private static long lastSeqNoInBitSet(final long bitSetKey) {
        return (1 + bitSetKey) * BIT_SET_SIZE - 1;
    }

    /**
     * Return the bit set for the provided sequence number, possibly allocating a new set if needed.
     *
     * @param seqNo the sequence number to obtain the bit set for
     * @return the bit set corresponding to the provided sequence number
     */
    private static long getBitSetKey(final long seqNo) {
        return seqNo / BIT_SET_SIZE;
    }

    private CountedBitSet getBitSetForSeqNo(final LongObjectHashMap<CountedBitSet> bitSetMap, final long seqNo) {
        assert Thread.holdsLock(this);
        final long bitSetKey = getBitSetKey(seqNo);
        final int index = bitSetMap.indexOf(bitSetKey);
        final CountedBitSet bitSet;
        if (bitSetMap.indexExists(index)) {
            bitSet = bitSetMap.indexGet(index);
        } else {
            bitSet = new CountedBitSet(BIT_SET_SIZE);
            bitSetMap.indexInsert(index, bitSetKey, bitSet);
        }
        return bitSet;
    }

    /**
     * Obtain the position in the bit set corresponding to the provided sequence number. The bit set corresponding to the sequence number
     * can be obtained via {@link #getBitSetForSeqNo(LongObjectHashMap, long)}.
     *
     * @param seqNo the sequence number to obtain the position for
     * @return the position in the bit set corresponding to the provided sequence number
     */
    private static int seqNoToBitSetOffset(final long seqNo) {
        return Math.toIntExact(seqNo % BIT_SET_SIZE);
    }

}
