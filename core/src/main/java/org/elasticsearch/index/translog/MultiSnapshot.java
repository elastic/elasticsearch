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

package org.elasticsearch.index.translog;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.LongSet;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

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
    public int overriddenOperations() {
        return overriddenOperations;
    }

    @Override
    public Translog.Operation next() throws IOException {
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
    public void close() throws IOException {
        onClose.close();
    }

    /**
     * A wrapper of {@link FixedBitSet} but allows to check if all bits are set in O(1).
     */
    private static final class CountedBitSet {
        private short onBits;
        private final FixedBitSet bitset;

        CountedBitSet(short numBits) {
            assert numBits > 0;
            this.onBits = 0;
            this.bitset = new FixedBitSet(numBits);
        }

        boolean getAndSet(int index) {
            assert index >= 0;
            boolean wasOn = bitset.getAndSet(index);
            if (wasOn == false) {
                onBits++;
            }
            return wasOn;
        }

        boolean hasAllBitsOn() {
            return onBits == bitset.length();
        }
    }

    /**
     * Sequence numbers from translog are likely to form contiguous ranges,
     * thus collapsing a completed bitset into a single entry will reduce memory usage.
     */
    static final class SeqNoSet {
        static final short BIT_SET_SIZE = 1024;
        private final LongSet completedSets = new LongHashSet();
        private final LongObjectHashMap<CountedBitSet> ongoingSets = new LongObjectHashMap<>();

        /**
         * Marks this sequence number and returns <tt>true</tt> if it is seen before.
         */
        boolean getAndSet(long value) {
            assert value >= 0;
            final long key = value / BIT_SET_SIZE;

            if (completedSets.contains(key)) {
                return true;
            }

            CountedBitSet bitset = ongoingSets.get(key);
            if (bitset == null) {
                bitset = new CountedBitSet(BIT_SET_SIZE);
                ongoingSets.put(key, bitset);
            }

            final boolean wasOn = bitset.getAndSet(Math.toIntExact(value % BIT_SET_SIZE));
            if (bitset.hasAllBitsOn()) {
                ongoingSets.remove(key);
                completedSets.add(key);
            }
            return wasOn;
        }

        // For testing
        long completeSetsSize() {
            return completedSets.size();
        }

        // For testing
        long ongoingSetsSize() {
            return ongoingSets.size();
        }
    }
}
