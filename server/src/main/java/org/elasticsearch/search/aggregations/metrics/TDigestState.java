/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.tdigest.Centroid;
import org.elasticsearch.tdigest.TDigest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Decorates {@link org.elasticsearch.tdigest.TDigest} with custom serialization. The underlying implementation for TDigest is selected
 * through factory method params, providing one optimized for performance (e.g. MergingDigest or HybridDigest) by default, or optionally one
 * that produces highly accurate results regardless of input size but its construction over the sample population takes 2x-10x longer.
 */
public class TDigestState {

    public static final Setting<String> EXECUTION_HINT = Setting.simpleString(
        "search.aggs.tdigest_execution_hint",
        "",
        TDigestExecutionHint::parse,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

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
     * Default factory for TDigestState. The underlying {@link org.elasticsearch.tdigest.TDigest} implementation is optimized for
     * performance, potentially providing slightly inaccurate results compared to other, substantially slower implementations.
     * @param compression the compression factor for the underlying {@link org.elasticsearch.tdigest.TDigest} object
     * @return a TDigestState object that's optimized for performance
     */
    public static TDigestState create(double compression) {
        return new TDigestState(Type.defaultValue(), compression);
    }

    /**
     * Factory for TDigestState that's optimized for high accuracy. It's substantially slower than the default implementation.
     * @param compression the compression factor for the underlying {@link org.elasticsearch.tdigest.TDigest} object
     * @return a TDigestState object that's optimized for performance
     */
    public static TDigestState createOptimizedForAccuracy(double compression) {
        return new TDigestState(Type.valueForHighAccuracy(), compression);
    }

    /**
     * Factory for TDigestState. The underlying {@link org.elasticsearch.tdigest.TDigest} implementation is either optimized for
     * performance (default), potentially providing slightly inaccurate results for large populations, or optimized for accuracy but taking
     * 2x-10x more to build.
     * @param compression the compression factor for the underlying {@link org.elasticsearch.tdigest.TDigest} object
     * @param executionHint controls which implementation is used; accepted values are 'high_accuracy' and '' (default)
     * @return a TDigestState object
     */
    public static TDigestState create(double compression, TDigestExecutionHint executionHint) {
        return switch (executionHint) {
            case HIGH_ACCURACY -> createOptimizedForAccuracy(compression);
            case DEFAULT -> create(compression);
            default -> throw new IllegalArgumentException(
                "Unexpected TDigestExecutionHint in TDigestState initialization: " + executionHint
            );
        };
    }

    /**
     * Factory for TDigestState. Uses the same initialization params as the passed TDigestState object. No data loading happens, and the
     * input TDigestState object doesn't get altered in any way.
     * @param state the TDigestState object providing the initialization params
     * @return a TDigestState object
     */
    public static TDigestState createUsingParamsFrom(TDigestState state) {
        return new TDigestState(state.type, state.compression);
    }

    protected TDigestState(Type type, double compression) {
        tdigest = switch (type) {
            case HYBRID -> TDigest.createHybridDigest(compression);
            case AVL_TREE -> TDigest.createAvlTreeDigest(compression);
            case SORTING -> TDigest.createSortingDigest();
            case MERGING -> TDigest.createMergingDigest(compression);
            default -> throw new IllegalArgumentException("Unexpected TDigestState type: " + type);
        };
        this.type = type;
        this.compression = compression;
    }

    public final double compression() {
        return compression;
    }

    public static void write(TDigestState state, StreamOutput out) throws IOException {
        out.writeDouble(state.compression);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_500_014)) {
            out.writeString(state.type.toString());
            out.writeVLong(state.tdigest.size());
        }

        out.writeVInt(state.centroidCount());
        for (Centroid centroid : state.centroids()) {
            out.writeDouble(centroid.mean());
            out.writeVLong(centroid.count());
        }
    }

    public static TDigestState read(StreamInput in) throws IOException {
        double compression = in.readDouble();
        TDigestState state;
        long size = 0;
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_500_014)) {
            state = new TDigestState(Type.valueOf(in.readString()), compression);
            size = in.readVLong();
        } else {
            state = new TDigestState(Type.valueForHighAccuracy(), compression);
        }
        int n = in.readVInt();
        if (size > 0) {
            state.tdigest.reserve(size);
        }
        for (int i = 0; i < n; i++) {
            state.add(in.readDouble(), in.readVInt());
        }
        return state;
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
            h = 31 * h + centroid.count();
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

    public void add(double x, int w) {
        tdigest.add(x, w);
    }

    public void add(double x) {
        tdigest.add(x, 1);
    }

    public void add(List<? extends TDigestState> others) {
        List<TDigest> otherTdigests = new ArrayList<>();
        for (TDigestState other : others) {
            otherTdigests.add(other.tdigest);
        }
        tdigest.add(otherTdigests);
    }

    public void add(TDigest other) {
        tdigest.add(other);
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
}
