/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import com.tdunning.math.stats.Centroid;

import org.elasticsearch.search.aggregations.metrics.TDigestState;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

final class QuantileState extends TDigestState {
    private static final VarHandle doubleHandle = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle intHandle = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

    QuantileState(double compression) {
        super(compression);
    }

    int estimateSizeInBytes() {
        return 12 + (12 * centroidCount());
    }

    int serialize(byte[] ba, int offset) {
        doubleHandle.set(ba, offset, compression());
        intHandle.set(ba, offset + 8, centroidCount());
        offset += 12;
        for (Centroid centroid : centroids()) {
            doubleHandle.set(ba, offset, centroid.mean());
            intHandle.set(ba, offset + 8, centroid.count());
            offset += 12;
        }
        return estimateSizeInBytes();
    }

    static QuantileState deserialize(byte[] ba, int offset) {
        final double compression = (double) doubleHandle.get(ba, offset);
        final QuantileState digest = new QuantileState(compression);
        final int positions = (int) intHandle.get(ba, offset + 8);
        offset += 12;
        for (int i = 0; i < positions; i++) {
            double mean = (double) doubleHandle.get(ba, offset);
            int count = (int) intHandle.get(ba, offset + 8);
            digest.add(mean, count);
            offset += 12;
        }
        return digest;
    }
}
