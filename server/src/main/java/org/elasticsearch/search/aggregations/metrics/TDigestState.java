/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tdigest.Centroid;
import org.elasticsearch.tdigest.MergingDigest;

import java.io.IOException;
import java.util.Iterator;

/**
 * Extension of {@link org.elasticsearch.tdigest.TDigest} with custom serialization.
 */
public class TDigestState extends MergingDigest {

    private final double compression;

    public TDigestState(double compression) {
        super(compression);
        this.compression = compression;
    }

    @Override
    public double compression() {
        return compression;
    }

    public static void write(TDigestState state, StreamOutput out) throws IOException {
        out.writeDouble(state.compression);
        out.writeVInt(state.centroidCount());
        for (Centroid centroid : state.centroids()) {
            out.writeDouble(centroid.mean());
            out.writeVLong(centroid.count());
        }
    }

    public static TDigestState read(StreamInput in) throws IOException {
        double compression = in.readDouble();
        TDigestState state = new TDigestState(compression);
        int n = in.readVInt();
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
        if (compression != that.compression) {
            return false;
        }
        if (this.getMax() != that.getMax()) {
            return false;
        }
        if (this.getMin() != that.getMin()) {
            return false;
        }
        if (this.isRecording() != that.isRecording()) {
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
        int h = 31 * Double.hashCode(compression);
        for (Centroid centroid : centroids()) {
            h = 31 * h + Double.hashCode(centroid.mean());
            h = 31 * h + centroid.count();
        }
        h = 31 * h + Double.hashCode(getMax());
        h = 31 * h + Double.hashCode(getMin());
        h = 31 * h + Boolean.hashCode(isRecording());
        return h;
    }
}
