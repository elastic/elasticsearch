/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.translog;

import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.index.seqno.CountedBitSet;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * A snapshot composed out of multiple snapshots
 */
final class MultiSnapshot implements Translog.Snapshot {

    private final TranslogSnapshot[] translogs;
    private final int totalOperations;
    private int overriddenOperations;
    private final Closeable onClose;
    private int index;
    private final SeqNoSet seenSeqNo;
    // Only used by next(): exploded operations of the most recently returned batch record.
    private final Deque<Translog.Operation> pendingExploded = new ArrayDeque<>();

    /**
     * Creates a new point in time snapshot of the given snapshots. Those snapshots are always iterated in-order.
     */
    MultiSnapshot(TranslogSnapshot[] translogs, Closeable onClose) {
        this.translogs = translogs;
        this.totalOperations = Arrays.stream(translogs).mapToInt(TranslogSnapshot::totalOperations).sum();
        this.overriddenOperations = 0;
        this.onClose = onClose;
        this.seenSeqNo = new SeqNoSet();
        this.index = translogs.length - 1;
    }

    @Override
    public int totalOperations() {
        return totalOperations;
    }

    @Override
    public int skippedOperations() {
        return Arrays.stream(translogs).mapToInt(TranslogSnapshot::skippedOperations).sum() + overriddenOperations;
    }

    @Override
    public Translog.Operation next() throws IOException {
        final Translog.Operation pending = pendingExploded.pollFirst();
        if (pending != null) {
            return pending;
        }
        Translog.Record record;
        while ((record = nextRecord()) != null) {
            if (record instanceof Translog.Operation op) {
                return op;
            }
            // overridden rows are already excluded from the exploded operations
            pendingExploded.addAll(((IndexOperationBatch.TranslogRecord) record).explode());
            final Translog.Operation first = pendingExploded.pollFirst();
            if (first != null) {
                return first;
            }
        }
        return null;
    }

    /**
     * Generations are read newest first, so a seqNo seen before belongs to a newer write that overrides
     * this one. A batch record is returned whole with its overridden rows marked skipped; the rows it
     * does replay are registered as seen before it is returned.
     */
    @Override
    public Translog.Record nextRecord() throws IOException {
        // TODO: Read translog forward in 9.0+
        for (; index >= 0; index--) {
            final TranslogSnapshot current = translogs[index];
            Translog.Record record;
            while ((record = current.nextRecord()) != null) {
                if (record instanceof Translog.Operation op) {
                    if (op.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO || seenSeqNo.getAndSet(op.seqNo()) == false) {
                        return op;
                    }
                    overriddenOperations++;
                    continue;
                }
                final IndexOperationBatch.TranslogRecord batch = (IndexOperationBatch.TranslogRecord) record;
                // filterRows tests each replayed row exactly once, so getAndSet both registers and filters
                final IndexOperationBatch.TranslogRecord kept = batch.filterRows(seqNo -> seenSeqNo.getAndSet(seqNo) == false);
                overriddenOperations += batch.replayCount() - (kept == null ? 0 : kept.replayCount());
                if (kept != null) {
                    return kept;
                }
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        onClose.close();
    }

    static final class SeqNoSet {
        static final short BIT_SET_SIZE = 1024;
        private final Map<Long, CountedBitSet> bitSets = new HashMap<>();

        /**
         * Marks this sequence number and returns {@code true} if it is seen before.
         */
        boolean getAndSet(long value) {
            assert value >= 0;
            final long key = value / BIT_SET_SIZE;
            CountedBitSet bitset = bitSets.get(key);
            if (bitset == null) {
                bitset = new CountedBitSet(BIT_SET_SIZE);
                bitSets.put(key, bitset);
            }
            final int index = Math.toIntExact(value % BIT_SET_SIZE);
            final boolean wasOn = bitset.get(index);
            bitset.set(index);
            return wasOn;
        }
    }
}
