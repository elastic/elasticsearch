/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

public final class TimeSeriesPartitionedHash implements PartitionedHashTable {
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
    private static final int KEY_SIZE = Long.BYTES * 2;
    private static final int PAGE_SHIFT = Integer.numberOfTrailingZeros(PageCacheRecycler.PAGE_SIZE_IN_BYTES);
    private static final int PAGE_MASK = PageCacheRecycler.PAGE_SIZE_IN_BYTES - 1;

    private final BytesRefSwissHash tsidHash;
    private final LongLongSwissHash pairHash;
    private final BigArrays bigArrays;

    public TimeSeriesPartitionedHash(BytesRefSwissHash tsidHash, LongLongSwissHash pairHash, BigArrays bigArrays) {
        this.tsidHash = tsidHash;
        this.pairHash = pairHash;
        this.bigArrays = bigArrays;
    }

    @Override
    public PartitionedHashKeys splitPartition(CircuitBreaker breaker, PartitionSplitter partitionSplitter) {
        final int numTsids = Math.toIntExact(tsidHash.size());
        final long usedBytes = (long) numTsids * (Byte.BYTES + Integer.BYTES);
        breaker.addEstimateBytesAndMaybeBreak(usedBytes, "TimeSeriesPartitionedHash#splitPartition");
        Keys keys = null;
        try {
            final byte[] partitions = new byte[numTsids];
            final int[] mappedOrdinals = new int[numTsids];
            keys = new Keys(breaker, Math.max(Math.ceilDiv(Math.toIntExact(pairHash.size()), NUM_PARTITIONS), 1));
            splitTsids(keys, partitions, mappedOrdinals);
            splitTimestamps(breaker, keys, partitions, mappedOrdinals, partitionSplitter);
            Keys result = keys;
            keys = null;
            return result;
        } finally {
            breaker.addWithoutBreaking(-usedBytes);
            if (keys != null) {
                keys.releaseAll(breaker);
            }
        }
    }

    private void splitTsids(Keys keys, byte[] partitions, int[] mappedOrdinals) {
        final BytesRefArray tsids = tsidHash.getBytesRefs();
        final int numTsids = partitions.length;
        for (int p = 0; p < NUM_PARTITIONS; p++) {
            keys.tsidKeys[p] = new BytesRefArray(Math.max(numTsids / NUM_PARTITIONS, 1), bigArrays, 16);
        }
        final BytesRef scratch = new BytesRef();
        for (int i = 0; i < numTsids; i++) {
            tsids.get(i, scratch);
            final int p = scratch.bytes[scratch.offset + scratch.length - 1] & PARTITION_MASK;
            partitions[i] = (byte) p;
            mappedOrdinals[i] = Math.toIntExact(keys.tsidKeys[p].size());
            keys.tsidKeys[p].append(scratch);
        }
    }

    private void splitTimestamps(CircuitBreaker breaker, Keys keys, byte[] partitions, int[] mappedOrdinals, PartitionSplitter splitter) {
        final int[] batchPartitionCounts = new int[NUM_PARTITIONS];
        final short[] shiftedIds = new short[PARTITION_WRITE_BATCH * NUM_PARTITIONS];
        final int[] partitionOffsets = keys.keyCounts;
        int batchStart = 0;
        final int size = Math.toIntExact(pairHash.size());
        final byte[][] keyPages = pairHash.keyPages();
        assert (long) size * KEY_SIZE <= (long) keyPages.length << PAGE_SHIFT : size + " keys in " + keyPages.length + " pages";
        int pageIndex = 0;
        byte[] keyPage = keyPages[0];
        int indexInPage = 0;
        for (int id = 0; id < size; id++) {
            if (indexInPage >= PageCacheRecycler.PAGE_SIZE_IN_BYTES) {
                keyPage = keyPages[++pageIndex];
                indexInPage = 0;
            }
            final long key1 = (long) LONG_HANDLE.get(keyPage, indexInPage);
            assert 0 <= key1 && key1 < partitions.length : key1;
            final int tsid = (int) key1;
            indexInPage += KEY_SIZE;
            final int p = partitions[tsid] & 0xFF;
            if (batchPartitionCounts[p] == PARTITION_WRITE_BATCH) {
                keys.splitKeys(breaker, keyPages, batchStart, shiftedIds, batchPartitionCounts, mappedOrdinals);
                splitter.split(batchStart, shiftedIds, id - batchStart, batchPartitionCounts, partitionOffsets);
                for (int i = 0; i < NUM_PARTITIONS; i++) {
                    partitionOffsets[i] += batchPartitionCounts[i];
                }
                batchStart = id;
                Arrays.fill(batchPartitionCounts, 0);
            }
            final int slot = p * PARTITION_WRITE_BATCH + batchPartitionCounts[p]++;
            assert id - batchStart <= Short.MAX_VALUE : id - batchStart;
            shiftedIds[slot] = (short) (id - batchStart);
        }
        keys.splitKeys(breaker, keyPages, batchStart, shiftedIds, batchPartitionCounts, mappedOrdinals);
        splitter.split(batchStart, shiftedIds, size - batchStart, batchPartitionCounts, partitionOffsets);
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            partitionOffsets[i] += batchPartitionCounts[i];
        }
    }

    @Override
    public boolean combinePartition(PartitionedHashKeys partitioned, int partition, int[] resultIds) {
        final Keys keys = (Keys) partitioned;
        final BytesRefArray tsidSlice = keys.tsidKeys[partition];
        final int numTsids = Math.toIntExact(tsidSlice.size());
        final int[] tsidOrds = new int[numTsids];
        tsidHash.mergeKeys(tsidSlice, numTsids, tsidOrds);
        final int numKeys = keys.keyCounts[partition];
        return pairHash.mergeKeys(keys.ordinalKeys[partition], tsidOrds, keys.timestampKeys[partition], resultIds, numKeys);
    }

    private static final class Keys implements PartitionedHashKeys {
        private static final int BYTES_PER_KEY = Integer.BYTES + Long.BYTES;
        final BytesRefArray[] tsidKeys = new BytesRefArray[NUM_PARTITIONS];
        final int[] keyCounts = new int[NUM_PARTITIONS];
        final int[][] ordinalKeys = new int[NUM_PARTITIONS][];
        final long[][] timestampKeys = new long[NUM_PARTITIONS][];

        Keys(CircuitBreaker breaker, int estimateKeysPerPartition) {
            final int capacity = ArrayUtil.oversize(estimateKeysPerPartition, Long.BYTES);
            breaker.addEstimateBytesAndMaybeBreak((long) NUM_PARTITIONS * capacity * BYTES_PER_KEY, "TimeSeriesPartitionedHash#keys");
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                ordinalKeys[p] = new int[capacity];
                timestampKeys[p] = new long[capacity];
            }
        }

        void splitKeys(CircuitBreaker breaker, byte[][] keyPages, int idOffset, short[] positions, int[] fills, int[] mappedOrdinals) {
            assert NUM_PARTITIONS * PARTITION_WRITE_BATCH < Short.MAX_VALUE : "shifted ids of one batch must fit in the short value range";
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                final int c = fills[p];
                if (c == 0) {
                    continue;
                }
                int writeOffset = keyCounts[p];
                ensureCapacity(breaker, p, writeOffset + c);
                final int[] ordinals = ordinalKeys[p];
                final long[] timestamps = timestampKeys[p];
                final int base = p * PARTITION_WRITE_BATCH;
                for (int i = 0; i < c; i++) {
                    final int id = idOffset + (positions[base + i] & 0xFFFF);
                    final long keyOffset = (long) id * KEY_SIZE;
                    final byte[] keyPage = keyPages[(int) (keyOffset >> PAGE_SHIFT)];
                    final int indexInPage = (int) (keyOffset & PAGE_MASK);
                    final long timestamp = (long) LONG_HANDLE.get(keyPage, indexInPage + Long.BYTES);
                    ordinals[writeOffset] = mappedOrdinals[(int) (long) LONG_HANDLE.get(keyPage, indexInPage)];
                    timestamps[writeOffset] = timestamp;
                    writeOffset++;
                }
            }
        }

        private void ensureCapacity(CircuitBreaker breaker, int p, int minLength) {
            final int currentLength = ordinalKeys[p].length;
            if (currentLength >= minLength) {
                return;
            }
            final int newLength = ArrayUtil.oversize(minLength, Long.BYTES);
            breaker.addEstimateBytesAndMaybeBreak((long) newLength * BYTES_PER_KEY, "TimeSeriesPartitionedHash#keys");
            ordinalKeys[p] = Arrays.copyOf(ordinalKeys[p], newLength);
            timestampKeys[p] = Arrays.copyOf(timestampKeys[p], newLength);
            breaker.addWithoutBreaking(-(long) currentLength * BYTES_PER_KEY);
        }

        @Override
        public int keysInPartition(int partition) {
            return keyCounts[partition];
        }

        @Override
        public void releasePartition(CircuitBreaker breaker, int partition) {
            breaker.addWithoutBreaking(-(long) ordinalKeys[partition].length * BYTES_PER_KEY);
            ordinalKeys[partition] = null;
            timestampKeys[partition] = null;
            Releasables.close(tsidKeys[partition]);
            tsidKeys[partition] = null;
        }

        @Override
        public void releaseAll(CircuitBreaker breaker) {
            long bytes = 0;
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                if (ordinalKeys[p] != null) {
                    bytes += (long) ordinalKeys[p].length * BYTES_PER_KEY;
                    ordinalKeys[p] = null;
                    timestampKeys[p] = null;
                }
            }
            breaker.addWithoutBreaking(-bytes);
            Releasables.close(tsidKeys);
            Arrays.fill(tsidKeys, null);
        }
    }
}
