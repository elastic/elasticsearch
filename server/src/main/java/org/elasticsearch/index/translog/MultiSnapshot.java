/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.translog;

import org.elasticsearch.index.seqno.CountedBitSet;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
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
    private final java.util.ArrayDeque<Translog.Operation> pendingOps = new java.util.ArrayDeque<>();

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
        // TODO: Read translog forward in 9.0+
        for (; index >= 0; index--) {
            final TranslogSnapshot current = translogs[index];
            Translog.Operation op;
            while ((op = current.next()) != null) {
                if (op.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO || seenSeqNo.getAndSet(op.seqNo()) == false) {
                    return op;
                } else {
                    overriddenOperations++;
                }
            }
        }
        return null;
    }

    @Override
    public Translog.Record nextRecord() throws IOException {
        // TODO: Read translog forward in 9.0+
        Translog.Operation bufferedOp = pendingOps.pollFirst();
        if (bufferedOp != null) {
            return bufferedOp;
        }
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
                final Translog.IndexBatch batch = (Translog.IndexBatch) record;
                // Check if the batch has seen sequence numbers or no-ops and explode. This simplifies
                // the logic while trying to index batch.
                if (seqNoSeenOrIsNoOp(batch)) {
                    for (Translog.Operation exploded : batch.explode()) {
                        if (exploded.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO || seenSeqNo.getAndSet(exploded.seqNo()) == false) {
                            pendingOps.addLast(exploded);
                        } else {
                            overriddenOperations++;
                        }
                    }
                    final Translog.Operation first = pendingOps.pollFirst();
                    if (first != null) {
                        return first;
                    }
                    // Everything in the batch has been seen. Continue reading the next record
                    continue;
                }
                for (Translog.IndexBatch.Op meta : batch.ops()) {
                    seenSeqNo.getAndSet(meta.seqNo());
                }
                return batch;
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        onClose.close();
    }

    private boolean seqNoSeenOrIsNoOp(Translog.IndexBatch indexBatch) {
        for (Translog.IndexBatch.Op op : indexBatch.ops()) {
            if (seenSeqNo.contains(op.seqNo()) || op instanceof Translog.IndexBatch.NoOpOp) {
                return true;
            }
        }
        return false;
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

        /** Returns true if this seqNo was already marked, without marking it. */
        boolean contains(long value) {
            assert value >= 0;
            final long key = value / BIT_SET_SIZE;
            final CountedBitSet bitset = bitSets.get(key);
            return bitset != null && bitset.get(Math.toIntExact(value % BIT_SET_SIZE));
        }
    }
}
