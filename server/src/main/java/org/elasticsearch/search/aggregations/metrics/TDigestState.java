/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tdigest.Centroid;
import org.elasticsearch.tdigest.TDigest;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

/**
 * Decorates {@link org.elasticsearch.tdigest.TDigest} with custom serialization. The underlying implementation for TDigest is selected
 * through factory method params, providing one optimized for performance (e.g. MergingDigest or HybridDigest) by default, or optionally one
 * that produces highly accurate results regardless of input size but its construction over the sample population takes 2x-10x longer.
 */
public class TDigestState implements Releasable, Accountable {
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TDigestState.class);

    private static final CircuitBreaker DEFAULT_NOOP_BREAKER = new NoopCircuitBreaker("default-tdigest-state-noop-breaker");

    private final CircuitBreaker breaker;
    private boolean closed = false;

    private final double compression;

    private final TDigest tdigest;

    // Supported tdigest types.
    protected enum Type {
        HYBRID,
        AVL_TREE,
        MERGING,
        SORTING;

        static Type defaultValue() {
            return HYBRID;
        }

        static Type valueForHighAccuracy() {
            return AVL_TREE;
        }
    }

    private final Type type;

    /**
     * @deprecated No-op circuit-breaked factory for TDigestState. Used in _search aggregations.
     *             Please use the {@link #create(CircuitBreaker, double)} method instead on new usages.
     */
    @Deprecated
    public static TDigestState createWithoutCircuitBreaking(double compression) {
        return create(DEFAULT_NOOP_BREAKER, compression);
    }

    /**
     * Default factory for TDigestState. The underlying {@link org.elasticsearch.tdigest.TDigest} implementation is optimized for
     * performance, potentially providing slightly inaccurate results compared to other, substantially slower implementations.
     * @param compression the compression factor for the underlying {@link org.elasticsearch.tdigest.TDigest} object
     * @return a TDigestState object that's optimized for performance
     */
    public static TDigestState create(CircuitBreaker breaker, double compression) {
        breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE, "tdigest-state-create");
        try {
            return new TDigestState(breaker, Type.defaultValue(), compression);
        } catch (Exception e) {
            breaker.addWithoutBreaking(-SHALLOW_SIZE);
            throw e;
        }
    }

    static TDigestState createOfType(CircuitBreaker breaker, Type type, double compression) {
        breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE, "tdigest-state-create-with-type");
        try {
            return new TDigestState(breaker, type, compression);
        } catch (Exception e) {
            breaker.addWithoutBreaking(-SHALLOW_SIZE);
            throw e;
        }
    }

    /**
     * Factory for TDigestState that's optimized for high accuracy. It's substantially slower than the default implementation.
     * @param compression the compression factor for the underlying {@link org.elasticsearch.tdigest.TDigest} object
     * @return a TDigestState object that's optimized for performance
     */
    static TDigestState createOptimizedForAccuracy(CircuitBreaker breaker, double compression) {
        breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE, "tdigest-state-create-optimized-for-accuracy");
        try {
            return new TDigestState(breaker, Type.valueForHighAccuracy(), compression);
        } catch (Exception e) {
            breaker.addWithoutBreaking(-SHALLOW_SIZE);
            throw e;
        }
    }

    /**
     * @deprecated No-op circuit-breaked factory for TDigestState. Used in _search aggregations.
     *             Please use the {@link #create(CircuitBreaker, double, TDigestExecutionHint)} method instead on new usages.
     */
    @Deprecated
    public static TDigestState createWithoutCircuitBreaking(double compression, TDigestExecutionHint executionHint) {
        return create(DEFAULT_NOOP_BREAKER, compression, executionHint);
    }

    /**
     * Factory for TDigestState. The underlying {@link org.elasticsearch.tdigest.TDigest} implementation is either optimized for
     * performance (default), potentially providing slightly inaccurate results for large populations, or optimized for accuracy but taking
     * 2x-10x more to build.
     * @param compression the compression factor for the underlying {@link org.elasticsearch.tdigest.TDigest} object
     * @param executionHint controls which implementation is used; accepted values are 'high_accuracy' and '' (default)
     * @return a TDigestState object
     */
    public static TDigestState create(CircuitBreaker breaker, double compression, TDigestExecutionHint executionHint) {
        return switch (executionHint) {
            case HIGH_ACCURACY -> createOptimizedForAccuracy(breaker, compression);
            case DEFAULT -> create(breaker, compression);
        };
    }

    /**
     * Factory for TDigestState. Uses the same initialization params as the passed TDigestState object. No data loading happens, and the
     * input TDigestState object doesn't get altered in any way.
     * @param state the TDigestState object providing the initialization params
     * @return a TDigestState object
     */
    public static TDigestState createUsingParamsFrom(TDigestState state) {
        state.breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE, "tdigest-state-create-using-params-from");
        try {
            return new TDigestState(state.breaker, state.type, state.compression);
        } catch (Exception e) {
            state.breaker.addWithoutBreaking(-SHALLOW_SIZE);
            throw e;
        }
    }

    protected TDigestState(CircuitBreaker breaker, Type type, double compression) {
        this.breaker = breaker;
        var arrays = new MemoryTrackingTDigestArrays(breaker);
        tdigest = switch (type) {
            case HYBRID -> TDigest.createHybridDigest(arrays, compression);
            case AVL_TREE -> TDigest.createAvlTreeDigest(arrays, compression);
            case SORTING -> TDigest.createSortingDigest(arrays);
            case MERGING -> TDigest.createMergingDigest(arrays, compression);
        };
        this.type = type;
        this.compression = compression;
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + tdigest.ramBytesUsed();
    }

    public final double compression() {
        return compression;
    }

    public static void write(TDigestState state, StreamOutput out) throws IOException {
        out.writeDouble(state.compression);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            out.writeString(state.type.toString());
            out.writeVLong(state.tdigest.size());
        }

        out.writeVInt(state.centroidCount());
        for (Centroid centroid : state.centroids()) {
            out.writeDouble(centroid.mean());
            out.writeVLong(centroid.count());
        }
    }

    /**
     * @deprecated No-op circuit-breaked factory for TDigestState. Used in _search aggregations.
     *             Please use the {@link #read(CircuitBreaker, StreamInput)} method instead on new usages.
     */
    @Deprecated
    public static TDigestState read(StreamInput in) throws IOException {
        return read(DEFAULT_NOOP_BREAKER, in);
    }

    public static TDigestState read(CircuitBreaker breaker, StreamInput in) throws IOException {
        double compression = in.readDouble();
        TDigestState state = null;
        long size = 0;
        boolean success = false;
        try {
            breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE, "tdigest-state-read");
            try {
                if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
                    state = new TDigestState(breaker, Type.valueOf(in.readString()), compression);
                    size = in.readVLong();
                } else {
                    state = new TDigestState(breaker, Type.valueForHighAccuracy(), compression);
                }
            } finally {
                if (state == null) {
                    breaker.addWithoutBreaking(-SHALLOW_SIZE);
                }
            }

            int n = in.readVInt();
            if (size > 0) {
                state.tdigest.reserve(size);
            }
            for (int i = 0; i < n; i++) {
                state.add(in.readDouble(), in.readVLong());
            }
            success = true;
            return state;
        } finally {
            if (success == false) {
                Releasables.close(state);
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TDigestState == false) {
            return false;
        }
        TDigestState that = (TDigestState) obj;
        if (this == that) {
            return true;
        }
        if (compression != that.compression) {
            return false;
        }
        if (type.equals(that.type) == false) {
            return false;
        }
        if (this.getMax() != that.getMax()) {
            return false;
        }
        if (this.getMin() != that.getMin()) {
            return false;
        }
        if (this.centroidCount() != that.centroidCount()) {
            return false;
        }

        Iterator<? extends Centroid> thisCentroids = centroids().iterator();
        Iterator<? extends Centroid> thatCentroids = that.centroids().iterator();
        while (thisCentroids.hasNext()) {
            if (thatCentroids.hasNext() == false) {
                return false;
            }
            Centroid thisNext = thisCentroids.next();
            Centroid thatNext = thatCentroids.next();
            if (thisNext.mean() != thatNext.mean() || thisNext.count() != thatNext.count()) {
                return false;
            }
        }
        return thatCentroids.hasNext() == false;
    }

    @Override
    public int hashCode() {
        int h = 31 * Double.hashCode(compression) + type.hashCode();
        h = 31 * h + Integer.hashCode(centroidCount());
        for (Centroid centroid : centroids()) {
            h = 31 * h + Double.hashCode(centroid.mean());
            h = 31 * h + (int) centroid.count();
        }
        h = 31 * h + Double.hashCode(getMax());
        h = 31 * h + Double.hashCode(getMin());
        return h;
    }

    /*
     * Expose the parts of the {@link org.elasticsearch.tdigest.TDigest} API that are used in the ES codebase. Refer to the TDigest
     * API documentation for each method below.
     */

    public void add(TDigestState other) {
        tdigest.add(other.tdigest);
    }

    public void add(double x, long w) {
        tdigest.add(x, w);
    }

    public void add(double x) {
        tdigest.add(x, 1);
    }

    public final void compress() {
        tdigest.compress();
    }

    public final long size() {
        return tdigest.size();
    }

    public final double cdf(double x) {
        return tdigest.cdf(x);
    }

    public final double quantile(double q) {
        return tdigest.quantile(q);
    }

    public final Collection<Centroid> centroids() {
        return tdigest.centroids();
    }

    public final int centroidCount() {
        return tdigest.centroidCount();
    }

    public final double getMin() {
        return tdigest.getMin();
    }

    public final double getMax() {
        return tdigest.getMax();
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            breaker.addWithoutBreaking(-SHALLOW_SIZE);
            Releasables.close(tdigest);
        }
    }
}
