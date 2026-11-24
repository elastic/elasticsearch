/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.Nullable;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_SCALE;

public class MergerFactoryImpl implements ExponentialHistogramMerger.Factory {

    /**
     * The smallest buffer capacity which is pooled, unless the bucket limit itself is smaller. Must be a power of two.
     */
    private static final int MIN_POOLED_BUFFER_CAPACITY = 16;
    private static final int MIN_POOLED_BUFFER_CAPACITY_LOG2 = Integer.numberOfTrailingZeros(MIN_POOLED_BUFFER_CAPACITY);
    static {
        assert Integer.bitCount(MIN_POOLED_BUFFER_CAPACITY) == 1 : "MIN_POOLED_BUFFER_CAPACITY must be a power of two";
    }

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(MergerFactoryImpl.class) + DownscaleStats.SIZE;

    private int activeMergers = 0;
    boolean isClosed = false;

    // The following temporary data is shared among mergers to reduce allocations and memory consumption.
    // pooledBuffers[i] holds a buffer with a capacity of 2^(i + MIN_POOLED_BUFFER_CAPACITY_LOG2) or null,
    // except for the last slot, which holds a buffer with a capacity of exactly bucketLimit.
    private final FixedCapacityExponentialHistogram[] pooledBuffers;

    final int bucketLimit;
    final int maxScale;

    final DownscaleStats downscaleStats;

    final ExponentialHistogramCircuitBreaker circuitBreaker;

    static MergerFactoryImpl create(int bucketLimit, int maxScale, ExponentialHistogramCircuitBreaker circuitBreaker) {
        long baseSize = estimateBaseSize(bucketLimit);
        circuitBreaker.adjustBreaker(baseSize);
        boolean success = false;
        MergerFactoryImpl result;
        try {
            result = new MergerFactoryImpl(bucketLimit, maxScale, circuitBreaker);
            success = true;
            return result;
        } finally {
            if (success == false) {
                circuitBreaker.adjustBreaker(-baseSize);
            }
        }
    }

    private MergerFactoryImpl(int bucketLimit, int maxScale, ExponentialHistogramCircuitBreaker circuitBreaker) {
        assert maxScale >= MIN_SCALE && maxScale <= MAX_SCALE : "max scale out of bounds";
        // We need at least four buckets to represent any possible distribution
        if (bucketLimit < 4) {
            throw new IllegalArgumentException("The bucket limit must be at least 4");
        }
        this.bucketLimit = bucketLimit;
        this.maxScale = maxScale;
        this.pooledBuffers = new FixedCapacityExponentialHistogram[numPools(bucketLimit)];
        this.circuitBreaker = circuitBreaker;
        this.downscaleStats = new DownscaleStats();
    }

    private static int numPools(int bucketLimit) {
        return uncappedPoolIndex(bucketLimit) + 1;
    }

    private static long estimateBaseSize(int bucketLimit) {
        return SHALLOW_SIZE + RamEstimationUtil.estimateObjectArray(numPools(bucketLimit));
    }

    /**
     * Returns the index of the pool holding buffers of the smallest power of two capacity which fits the given number of buckets,
     * without accounting for the bucket limit.
     */
    private static int uncappedPoolIndex(int requiredBucketCount) {
        int ceilLog2 = 32 - Integer.numberOfLeadingZeros(Math.max(requiredBucketCount, 1) - 1);
        return Math.max(0, ceilLog2 - MIN_POOLED_BUFFER_CAPACITY_LOG2);
    }

    private int poolIndex(int requiredBucketCount) {
        return Math.min(uncappedPoolIndex(requiredBucketCount), pooledBuffers.length - 1);
    }

    private int poolCapacity(int poolIndex) {
        return poolIndex == pooledBuffers.length - 1 ? bucketLimit : 1 << (poolIndex + MIN_POOLED_BUFFER_CAPACITY_LOG2);
    }

    @Override
    public ExponentialHistogramMerger createMerger() {
        assert isClosed == false : "factory is closed";
        ExponentialHistogramMerger result = ExponentialHistogramMerger.create(this);
        activeMergers++;
        return result;
    }

    protected void maybeReleaseMemory() {
        if (isClosed && activeMergers == 0) {
            circuitBreaker.adjustBreaker(-estimateBaseSize(bucketLimit));
            for (int i = 0; i < pooledBuffers.length; i++) {
                if (pooledBuffers[i] != null) {
                    pooledBuffers[i].close();
                    pooledBuffers[i] = null;
                }
            }
        }
    }

    /**
     * Acquires a buffer which is able to hold at least the given number of buckets.
     * The capacity of the returned buffer is capped at the {@link #bucketLimit}, so callers must be prepared to
     * deal with the buffer being too small if {@code requiredBucketCount} exceeds the bucket limit.
     *
     * @param requiredBucketCount the number of buckets the buffer should be able to hold
     * @return the buffer, which must be returned via {@link #releaseBuffer(FixedCapacityExponentialHistogram)}
     */
    FixedCapacityExponentialHistogram acquireBuffer(int requiredBucketCount) {
        int poolIndex = poolIndex(requiredBucketCount);
        FixedCapacityExponentialHistogram result = pooledBuffers[poolIndex];
        pooledBuffers[poolIndex] = null;
        if (result == null) {
            result = FixedCapacityExponentialHistogram.create(poolCapacity(poolIndex), circuitBreaker);
        }
        return result;
    }

    /**
     * Release the buffer back to the factory.
     * @param buffer the buffer previously acquired via {@link #acquireBuffer(int)}, or null in which case this is a no-op
     */
    void releaseBuffer(@Nullable FixedCapacityExponentialHistogram buffer) {
        if (buffer == null) {
            return;
        }
        int poolIndex = poolIndex(buffer.getCapacity());
        assert poolCapacity(poolIndex) == buffer.getCapacity() : "buffer was not acquired from this factory";
        if (pooledBuffers[poolIndex] == null) {
            pooledBuffers[poolIndex] = buffer;
        } else {
            // release the buffer as we already have one of this size
            buffer.close();
        }
    }

    void notifyMergerClosed() {
        assert activeMergers > 0 : "no active mergers to close";
        activeMergers--;
        maybeReleaseMemory();
    }

    @Override
    public void close() {
        assert isClosed == false : "factory is already closed";
        if (isClosed == false) {
            isClosed = true;
            maybeReleaseMemory();
        }
    }

    @Override
    public long ramBytesUsed() {
        long size = estimateBaseSize(bucketLimit);
        for (FixedCapacityExponentialHistogram buffer : pooledBuffers) {
            if (buffer != null) {
                size += buffer.ramBytesUsed();
            }
        }
        return size;
    }
}
