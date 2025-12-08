/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * This exists to hold the values from a {@link TDigestBlock}.  It is roughly parallel to
 * {@link org.elasticsearch.search.aggregations.metrics.TDigestState} in classic aggregations, which we are not using directly because
 * the serialization format is pretty bad for ESQL's use case (specifically, encoding the near-constant compression and merge strategy
 * data inline as opposed to in a dedicated column isn't great).
 */
public class TDigestHolder {

    private final double min;
    private final double max;
    private final double sum;
    private final long valueCount;
    private final BytesRef encodedDigest;

    // TODO - Deal with the empty array case better
    public TDigestHolder(BytesRef encodedDigest, double min, double max, double sum, long valueCount) {
        this.encodedDigest = encodedDigest;
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.valueCount = valueCount;
    }

    public TDigestHolder(List<Double> centroids, List<Long> counts, double min, double max, double sum, long valueCount)
        throws IOException {
        this(encodeCentroidsAndCounts(centroids, counts), min, max, sum, valueCount);
    }

    @Override
    public boolean equals(Object o) {
        if ((o instanceof TDigestHolder that)) {
            return Double.compare(min, that.min) == 0
                && Double.compare(max, that.max) == 0
                && Double.compare(sum, that.sum) == 0
                && valueCount == that.valueCount
                && Objects.equals(encodedDigest, that.encodedDigest);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max, sum, valueCount, encodedDigest);
    }

    private static BytesRef encodeCentroidsAndCounts(List<Double> centroids, List<Long> counts) throws IOException {
        // TODO: This is copied from the method of the same name in TDigestFieldMapper. It would be nice to find a way to reuse that code
        BytesStreamOutput streamOutput = new BytesStreamOutput();

        for (int i = 0; i < centroids.size(); i++) {
            long count = counts.get(i);
            assert count >= 0;
            // we do not add elements with count == 0
            if (count > 0) {
                streamOutput.writeVLong(count);
                streamOutput.writeDouble(centroids.get(i));
            }
        }

        BytesRef docValue = streamOutput.bytes().toBytesRef();
        return docValue;
    }

    public BytesRef getEncodedDigest() {
        return encodedDigest;
    }

    // TODO - compute these if they're not given? or do that at object creation time, maybe.
    public double getMax() {
        return max;
    }

    public double getMin() {
        return min;
    }

    public double getSum() {
        return sum;
    }

    public long getValueCount() {
        return valueCount;
    }

}
