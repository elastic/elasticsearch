/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.PartitionedHashTable;

import java.util.List;

public final class BytesLongPartitionedHash implements PartitionedHashTable {
    /*
     * longValue, intValue  -> longValue, intValue & WIDEN
     * null, intValue       -> 0, intValue & WIDEN | LONG_NULL_MASK
     * longValue, null      -> longValue, INT_NULL_MASK
     * null, null           -> 0, LONG_NULL_MASK | INT_NULL_MASK
     */
    public static final long LONG_NULL_MASK = 0x00F0_0000_0000_0000L;
    public static final long INT_NULL_MASK = 0x000F_0000_0000_0000L;
    public static final long WIDEN = 0xFFFF_FFFFL;

    private static final PartitionSplitter NO_SPLITTER = new PartitionSplitter() {
        @Override
        public void split(int firstId, short[] shiftedIds, int batchSize, int[] batchPartitionCounts, int[] partitionOffsets) {}

        @Override
        public void release(CircuitBreaker breaker) {}
    };

    final BytesRefSwissHash bytesHash;
    final LongLongSwissHash longlongHash;

    public BytesLongPartitionedHash(BytesRefSwissHash bytesHash, LongLongSwissHash longlongHash) {
        this.bytesHash = bytesHash;
        this.longlongHash = longlongHash;
    }

    @Override
    public PartitionedHashKeys splitPartition(CircuitBreaker breaker, PartitionSplitter partitionSplitter) {
        final int numIds = bytesHash.size;
        final long routesBytes = (long) numIds * (Byte.BYTES + Integer.BYTES);
        breaker.addEstimateBytesAndMaybeBreak(routesBytes, "BytesLongPartitionedHash#split");
        PartitionedHashKeys bytesKeys = null;
        try {
            final byte[] dictionaryPartitions = new byte[numIds];
            bytesKeys = bytesHash.splitPartition(breaker, dictionaryPartitions, NO_SPLITTER);
            var longKeys = longlongHash.splitPartition(breaker, new DictionaryPartitions(dictionaryPartitions), partitionSplitter);
            mapToLocalOrds(longKeys, dictionaryPartitions);
            BytesAndLongKeys combinedKeys = new BytesAndLongKeys(bytesKeys, longKeys);
            bytesKeys = null;
            return combinedKeys;
        } finally {
            breaker.addWithoutBreaking(-routesBytes);
            if (bytesKeys != null) {
                bytesKeys.releaseAll(breaker);
            }
        }
    }

    record DictionaryPartitions(byte[] partitions) implements LongLongSwissHash.Key2Partitioner {
        @Override
        public int partition(long key2) {
            return (key2 & INT_NULL_MASK) != 0 ? 0 : partitions[(int) key2] & 0xFF;
        }
    }

    private static void mapToLocalOrds(LongLongSwissHash.LongLongPartitionedHashKeys longKeys, byte[] partitions) {
        final int[] positions = new int[partitions.length];
        final int[] nextPosition = new int[NUM_PARTITIONS];
        for (int id = 0; id < positions.length; id++) {
            positions[id] = nextPosition[partitions[id] & 0xFF]++;
        }
        for (int p = 0; p < NUM_PARTITIONS; p++) {
            mapOnePartitionKeys(longKeys.partitionKeys[p], longKeys.keysInPartition(p), positions);
        }
    }

    private static void mapOnePartitionKeys(long[] keys, int numKeys, int[] newOrds) {
        final int end = numKeys * 2;
        for (int idx = 1; idx < end; idx += 2) {
            final long key2 = keys[idx];
            if ((key2 & INT_NULL_MASK) == 0) {
                keys[idx] = (key2 & ~WIDEN) | newOrds[(int) key2];
            }
        }
    }

    record BytesAndLongKeys(PartitionedHashKeys bytesKeys, LongLongSwissHash.LongLongPartitionedHashKeys longKeys)
        implements
            PartitionedHashKeys {

        @Override
        public int keysInPartition(int partition) {
            return longKeys.keysInPartition(partition);
        }

        @Override
        public void releasePartition(CircuitBreaker breaker, int partition) {
            bytesKeys.releasePartition(breaker, partition);
            longKeys.releasePartition(breaker, partition);
        }

        @Override
        public void releaseAll(CircuitBreaker breaker) {
            bytesKeys.releaseAll(breaker);
            longKeys.releaseAll(breaker);
        }
    }

    @Override
    public boolean combinePartition(PartitionedHashKeys partitioned, int partitionIndex, int[] resultIds) {
        BytesAndLongKeys combined = (BytesAndLongKeys) partitioned;
        combineBytes(combined, partitionIndex, resultIds);
        return longlongHash.combinePartition(combined.longKeys, partitionIndex, resultIds);
    }

    @Override
    public boolean[] combinePartitions(List<? extends PartitionedHashKeys> partitioned, int partitionIndex, int[][] resultIds) {
        final int numGens = partitioned.size();
        for (int g = 0; g < numGens; g++) {
            BytesAndLongKeys combined = (BytesAndLongKeys) partitioned.get(g);
            if (combined.longKeys.keysInPartition(partitionIndex) > 0) {
                combineBytes(combined, partitionIndex, resultIds[g]);
            }
        }
        final boolean[] appendOnly = new boolean[numGens];
        for (int g = 0; g < numGens; g++) {
            BytesAndLongKeys combined = (BytesAndLongKeys) partitioned.get(g);
            appendOnly[g] = combined.longKeys.keysInPartition(partitionIndex) == 0
                || longlongHash.combinePartition(combined.longKeys, partitionIndex, resultIds[g]);
        }
        return appendOnly;
    }

    private void combineBytes(BytesAndLongKeys combined, int partitionIndex, int[] scratch) {
        final int numBytes = combined.bytesKeys.keysInPartition(partitionIndex);
        assert numBytes <= scratch.length : numBytes + " > " + scratch.length;
        bytesHash.combinePartition(combined.bytesKeys, partitionIndex, scratch);
        final var longKeys = combined.longKeys;
        mapOnePartitionKeys(longKeys.partitionKeys[partitionIndex], longKeys.keysInPartition(partitionIndex), scratch);
    }
}
