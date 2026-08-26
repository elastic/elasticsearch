/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

public interface PartitionedHashTable {
    int NUM_PARTITIONS = 256;
    int PARTITION_MASK = NUM_PARTITIONS - 1;
    int SPLIT_WRITE_BATCH_SIZE = 16;

    interface PartitionedKeys extends Releasable {
        void releasePartition(int partition);

        int partitionSize(int partition);
    }

    interface PartitionedAgg extends Releasable {
        void releasePartition(int partition);
    }

    class ScratchBuffer {
        public long[] longs = new long[0];
        public double[] doubles = new double[0];
        public int splitWriteBatchSize = SPLIT_WRITE_BATCH_SIZE;
    }

    interface AggSplitter extends Releasable {
        /**
         * Called once before the first {@link #split} call with the exact number of keys that will
         * be routed to each partition, allowing splitters to pre-allocate per-partition output
         * arrays without any growth. The default implementation is a no-op.
         */
        default void preAllocate(int[] partitionCounts) {}

        void split(PartitionedHashTable.ScratchBuffer scratch, int idOffset, int batchLen, short[] positions, int[] fills);

        PartitionedAgg finish();
    }

    record PartitionedKeysAndAggs(PartitionedKeys keys, PartitionedAgg aggs) implements Releasable {
        @Override
        public void close() {
            Releasables.close(keys, aggs);
        }
    }

    PartitionedKeys partition(BigArrays bigArrays, CircuitBreaker breaker, AggSplitter aggSplitter);

    class MergedKeys implements Releasable {
        public int[] ids;
        public int length;

        // bool -> indicate non-repeated -> allow append
        public void ensureCapacity(int capacity) {
            if (ids == null || ids.length < capacity) {
                ids = new int[ArrayUtil.oversize(capacity, Integer.BYTES)];
            }
        }

        @Override
        public void close() {
            ids = null;
            length = 0;
        }
    }

    MergedKeys mergeKeys(PartitionedKeys keys, int partition, int estimateTotalSize, MergedKeys reused);
}
