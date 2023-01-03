/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.TDigest;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Extension of {@link com.tdunning.math.stats.TDigest} with custom serialization.
 */
public class TDigestState extends TDigest {
    private final TDigest delegate;

    private static VariantStage withCompression(double compression) {
        return new Builder(compression);
    }

    public TDigestState(double compression) {
        this.delegate = TDigestState.withCompression(compression).defaultTDigest();
    }

    private TDigestState(final TDigest delegate) {
        this.delegate = delegate;
    }

    @Override
    public void add(double x, int w) {
        this.delegate.add(x, w);
    }

    @Override
    public void add(List<? extends TDigest> others) {
        this.delegate.add(others);
    }

    @Override
    public void compress() {
        this.delegate.compress();
    }

    @Override
    public long size() {
        return this.delegate.size();
    }

    @Override
    public double cdf(double x) {
        return this.delegate.cdf(x);
    }

    @Override
    public double quantile(double q) {
        return this.delegate.quantile(q);
    }

    @Override
    public Collection<Centroid> centroids() {
        return this.delegate.centroids();
    }

    @Override
    public double compression() {
        return this.delegate.compression();
    }

    @Override
    public int byteSize() {
        return this.delegate.byteSize();
    }

    @Override
    public int smallByteSize() {
        return this.delegate.smallByteSize();
    }

    @Override
    public void asBytes(ByteBuffer buf) {
        this.delegate.asBytes(buf);
    }

    @Override
    public void asSmallBytes(ByteBuffer buf) {
        this.delegate.asSmallBytes(buf);
    }

    @Override
    public TDigest recordAllData() {
        return this.delegate.recordAllData();
    }

    @Override
    public boolean isRecording() {
        return this.delegate.isRecording();
    }

    @Override
    public void add(double x) {
        this.delegate.add(x);
    }

    @Override
    public void add(TDigest other) {
        this.delegate.add(other);
    }

    @Override
    public int centroidCount() {
        return this.delegate.centroidCount();
    }

    public static void write(TDigestState state, StreamOutput out) throws IOException {
        out.writeDouble(state.compression());
        out.writeVInt(state.centroidCount());
        for (Centroid centroid : state.centroids()) {
            out.writeDouble(centroid.mean());
            out.writeVInt(centroid.count());
        }
    }

    public static TDigestState read(StreamInput in) throws IOException {
        double compression = in.readDouble();
        TDigestState state = in.getVersion().onOrAfter(Version.V_8_7_0)
            ? TDigestState.withCompression(compression).mergingTDigest()
            : TDigestState.withCompression(compression).defaultTDigest();
        int n = in.readVInt();
        for (int i = 0; i < n; i++) {
            state.add(in.readDouble(), in.readVInt());
        }
        return state;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj instanceof TDigestState == false) {
            return false;
        }
        TDigestState that = (TDigestState) obj;
        if (this.compression() != that.compression()) {
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
        int h = getClass().hashCode();
        h = 31 * h + Double.hashCode(compression());
        for (Centroid centroid : centroids()) {
            h = 31 * h + Double.hashCode(centroid.mean());
            h = 31 * h + centroid.count();
        }
        return h;
    }

    public interface VariantStage {
        TDigestState defaultTDigest();

        TDigestState avlTDigest();

        TDigestState mergingTDigest();
    }

    public static class Builder implements VariantStage {

        private final double compression;

        public Builder(double compression) {
            this.compression = compression;
        }

        @Override
        public TDigestState defaultTDigest() {
            return new TDigestState(TDigest.createAvlTreeDigest(this.compression));
        }

        @Override
        public TDigestState avlTDigest() {
            return new TDigestState(TDigest.createAvlTreeDigest(this.compression));
        }

        @Override
        public TDigestState mergingTDigest() {
            return new TDigestState(TDigest.createMergingDigest(this.compression));
        }
    }
}
