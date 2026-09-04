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

    private static final long BASE_SIZE = RamUsageEstimator.shallowSizeOfInstance(MergerFactoryImpl.class) + DownscaleStats.SIZE;

    private int activeMergers = 0;
    boolean isClosed = false;

    // The following temporary data is shared among mergers to reduce allocations and memory consumption.
    @Nullable
    private FixedCapacityExponentialHistogram buffer;

    final int bucketLimit;
    final int maxScale;

    final DownscaleStats downscaleStats;

    final ExponentialHistogramCircuitBreaker circuitBreaker;

    static MergerFactoryImpl create(int bucketLimit, int maxScale, ExponentialHistogramCircuitBreaker circuitBreaker) {
        circuitBreaker.adjustBreaker(BASE_SIZE);
        boolean success = false;
        MergerFactoryImpl result;
        try {
            result = new MergerFactoryImpl(bucketLimit, maxScale, circuitBreaker);
            success = true;
            return result;
        } finally {
            if (success == false) {
                circuitBreaker.adjustBreaker(-BASE_SIZE);
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
        this.circuitBreaker = circuitBreaker;
        this.downscaleStats = new DownscaleStats();
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
            circuitBreaker.adjustBreaker(-BASE_SIZE);
            if (buffer != null) {
                buffer.close();
                buffer = null;
            }
        }
    }

    FixedCapacityExponentialHistogram acquireBuffer() {
        FixedCapacityExponentialHistogram result = buffer;
        buffer = null;
        if (result == null) {
            result = FixedCapacityExponentialHistogram.create(bucketLimit, circuitBreaker);
        }
        return result;
    }

    /**
     * Release the buffer back to the factory.
     * @param buffer the buffer previously acquired via {@link #acquireBuffer()}
     */
    void releaseBuffer(FixedCapacityExponentialHistogram buffer) {
        if (this.buffer == null) {
            this.buffer = buffer;
        } else {
            // release the buffer as we already have one
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
        long size = BASE_SIZE;
        if (buffer != null) {
            size += buffer.ramBytesUsed();
        }
        return size;
    }
}
