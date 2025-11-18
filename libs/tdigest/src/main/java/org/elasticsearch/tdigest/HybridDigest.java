/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
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
 */

package org.elasticsearch.tdigest;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tdigest.arrays.TDigestArrays;

import java.util.Collection;

/**
 * Uses a {@link SortingDigest} implementation under the covers for small sample populations, then switches to {@link MergingDigest}.
 * The {@link SortingDigest} is perfectly accurate and the fastest implementation for up to millions of samples, at the cost of increased
 * memory footprint as it tracks all samples. Conversely, the {@link MergingDigest} pre-allocates its memory (tens of KBs) and provides
 * better performance for hundreds of millions of samples and more, while accuracy stays bounded to 0.1-1% for most cases.
 *
 * This hybrid  approach provides the best of both worlds, i.e. speedy and accurate percentile calculations for small populations with
 * bounded memory allocation and acceptable speed and accuracy for larger ones.
 */
public class HybridDigest extends AbstractTDigest {
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(HybridDigest.class);

    private final TDigestArrays arrays;
    private boolean closed = false;

    // See MergingDigest's compression param.
    private final double compression;

    // Indicates the sample size over which it switches from SortingDigest to MergingDigest.
    private final long maxSortingSize;

    // This is set to null when the implementation switches to MergingDigest.
    private SortingDigest sortingDigest;

    // This gets initialized when the implementation switches to MergingDigest.
    private MergingDigest mergingDigest;

    static HybridDigest create(TDigestArrays arrays, double compression) {
        arrays.adjustBreaker(SHALLOW_SIZE);
        try {
            return new HybridDigest(arrays, compression);
        } catch (Exception e) {
            arrays.adjustBreaker(-SHALLOW_SIZE);
            throw e;
        }
    }

    /**
     * Creates a hybrid digest that uses a {@link SortingDigest} for up to {@param maxSortingSize} samples,
     * then switches to a {@link MergingDigest}.
     *
     * @param compression The compression factor for the MergingDigest
     * @param maxSortingSize The sample size limit for switching from a {@link SortingDigest} to a {@link MergingDigest} implementation
     */
    private HybridDigest(TDigestArrays arrays, double compression, long maxSortingSize) {
        this.arrays = arrays;
        this.compression = compression;
        this.maxSortingSize = maxSortingSize;
        this.sortingDigest = TDigest.createSortingDigest(arrays);
    }

    /**
     * Similar to the constructor above. The limit for switching from a {@link SortingDigest} to a {@link MergingDigest} implementation
     * is calculated based on the passed compression factor.
     *
     * @param compression The compression factor for the MergingDigest
     */
    private HybridDigest(TDigestArrays arrays, double compression) {
        // The default maxSortingSize is calculated so that the SortingDigest will have comparable size with the MergingDigest
        // at the point where implementations switch, e.g. for default compression 100 SortingDigest allocates ~16kB and MergingDigest
        // allocates ~15kB.
        this(arrays, compression, Math.round(compression) * 20);
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + (sortingDigest != null ? sortingDigest.ramBytesUsed() : 0) + (mergingDigest != null
            ? mergingDigest.ramBytesUsed()
            : 0);
    }

    @Override
    public void add(double x, long w) {
        reserve(w);
        if (mergingDigest != null) {
            mergingDigest.add(x, w);
        } else {
            sortingDigest.add(x, w);
        }
    }

    @Override
    public void add(TDigest other) {
        reserve(other.size());
        if (mergingDigest != null) {
            mergingDigest.add(other);
        } else {
            sortingDigest.add(other);
        }
    }

    @Override
    public void reserve(long size) {
        if (mergingDigest != null) {
            mergingDigest.reserve(size);
            return;
        }
        // Check if we need to switch implementations.
        assert sortingDigest != null;
        if (sortingDigest.size() + size >= maxSortingSize) {
            mergingDigest = TDigest.createMergingDigest(arrays, compression);
            for (int i = 0; i < sortingDigest.values.size(); i++) {
                mergingDigest.add(sortingDigest.values.get(i));
            }
            mergingDigest.reserve(size);
            // Release the allocated SortingDigest.
            sortingDigest.close();
            sortingDigest = null;
        } else {
            sortingDigest.reserve(size);
        }
    }

    @Override
    public void compress() {
        if (mergingDigest != null) {
            mergingDigest.compress();
        } else {
            sortingDigest.compress();
        }
    }

    @Override
    public long size() {
        if (mergingDigest != null) {
            return mergingDigest.size();
        }
        return sortingDigest.size();
    }

    @Override
    public double cdf(double x) {
        if (mergingDigest != null) {
            return mergingDigest.cdf(x);
        }
        return sortingDigest.cdf(x);
    }

    @Override
    public double quantile(double q) {
        if (mergingDigest != null) {
            return mergingDigest.quantile(q);
        }
        return sortingDigest.quantile(q);
    }

    @Override
    public Collection<Centroid> centroids() {
        if (mergingDigest != null) {
            return mergingDigest.centroids();
        }
        return sortingDigest.centroids();
    }

    @Override
    public double compression() {
        if (mergingDigest != null) {
            return mergingDigest.compression();
        }
        return sortingDigest.compression();
    }

    @Override
    public int centroidCount() {
        if (mergingDigest != null) {
            return mergingDigest.centroidCount();
        }
        return sortingDigest.centroidCount();
    }

    @Override
    public double getMin() {
        if (mergingDigest != null) {
            return mergingDigest.getMin();
        }
        return sortingDigest.getMin();
    }

    @Override
    public double getMax() {
        if (mergingDigest != null) {
            return mergingDigest.getMax();
        }
        return sortingDigest.getMax();
    }

    @Override
    public int byteSize() {
        if (mergingDigest != null) {
            return mergingDigest.byteSize();
        }
        return sortingDigest.byteSize();
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            arrays.adjustBreaker(-SHALLOW_SIZE);
            Releasables.close(sortingDigest, mergingDigest);
        }
    }
}
