/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.CollectedAggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CollectedMax extends CollectedAggregator {
    private DoubleArray maxes;
    private DocValueFormat format;

    protected CollectedMax(String name, Map<String, Object> metadata, BigArrays bigArrays, long size, DocValueFormat format) {
        super(name, metadata, bigArrays);
        maxes = bigArrays().newDoubleArray(size, false);
        this.format = format;
    }

    public CollectedMax(StreamInput in) throws IOException {
        super(in);
        // TODO: Read the buffer backed big array here
    }

    @Override
    public void close() {
        Releasables.close(maxes);
    }

    @Override
    public String getWriteableName() {
        return MaxAggregationBuilder.NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        // NOCOMMIT: put the correct version number here
        return Version.V_8_5_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public CollectedAggregator reduce(List<CollectedAggregator> aggregations, AggregationReduceContext reduceContext) {
        CollectedMax reducedMax = new CollectedMax(name, metadata, bigArrays(), aggregations.size(), format);
        reducedMax.fill(Double.NEGATIVE_INFINITY);
        for (CollectedAggregator aggregation : aggregations) {
            if (aggregation instanceof CollectedMax other) {
                assert other.size() == size();
                for (int i = 0; i < size(); i++) {
                    reducedMax.set(i, Math.max(reducedMax.get(i), other.get(i)));
                }
            } else {
                // this should never happen, right?
                throw new AggregationExecutionException("Mixed type reduction! Expected [" + this.getClass().getSimpleName() + "] got [" +
                    aggregation.getClass().getSimpleName() + "]");
            }
            // In theory, the aggregation we just processed could be released here.
        }
        return reducedMax;
    }

    private void fill(double value) {
        this.maxes.fill(0, maxes.size() - 1, value);
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    @Override
    public InternalAggregation[] convertToLegacy(int[] bucketOrdinal) {
        Max[] legacyMaxes = new Max[bucketOrdinal.length];
        for (int ord = 0; ord < bucketOrdinal.length; ord++ ) {
            legacyMaxes[ord] = new Max(name, maxes.get(bucketOrdinal[ord]), format, metadata);
        }
        return legacyMaxes;
    }

    public double get(long bucketOrd) {
        return maxes.get(bucketOrd);
    }

    public void set(long bucketOrd, double value) {
        if (bucketOrd >= maxes.size()) {
            long from = maxes.size();
            maxes = bigArrays().grow(maxes, bucketOrd + 1);
            maxes.fill(from, maxes.size(), Double.NEGATIVE_INFINITY);
        }
        // We could actually check that what we're setting is bigger than the current value, but that seems against the spirit of a setter
        maxes.set(bucketOrd, value);
    }

    public long size() {
        return maxes.size();
    }
}
