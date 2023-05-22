/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * A metric aggregator that computes a cartesian-bounds from a {@code point} type field
 */
public abstract class CartesianBoundsAggregatorBase extends MetricsAggregator {
    private DoubleArray tops;
    private DoubleArray bottoms;
    private DoubleArray lefts;
    private DoubleArray rights;

    public CartesianBoundsAggregatorBase(String name, AggregationContext context, Aggregator parent, Map<String, Object> metadata)
        throws IOException {
        super(name, context, parent, metadata);
        tops = bigArrays().newDoubleArray(1, false);
        tops.fill(0, tops.size(), Double.NEGATIVE_INFINITY);
        bottoms = bigArrays().newDoubleArray(1, false);
        bottoms.fill(0, bottoms.size(), Double.POSITIVE_INFINITY);
        lefts = bigArrays().newDoubleArray(1, false);
        lefts.fill(0, lefts.size(), Double.POSITIVE_INFINITY);
        rights = bigArrays().newDoubleArray(1, false);
        rights.fill(0, rights.size(), Double.NEGATIVE_INFINITY);
    }

    protected void addBounds(long bucket, double top, double bottom, double left, double right) {
        tops.set(bucket, max(tops.get(bucket), top));
        bottoms.set(bucket, min(bottoms.get(bucket), bottom));
        lefts.set(bucket, min(lefts.get(bucket), left));
        rights.set(bucket, max(rights.get(bucket), right));
    }

    protected void maybeResize(long bucket) {
        if (bucket >= tops.size()) {
            final long from = tops.size();
            tops = bigArrays().grow(tops, bucket + 1);
            tops.fill(from, tops.size(), Double.NEGATIVE_INFINITY);
            bottoms = bigArrays().resize(bottoms, tops.size());
            bottoms.fill(from, bottoms.size(), Double.POSITIVE_INFINITY);
            lefts = bigArrays().resize(lefts, tops.size());
            lefts.fill(from, lefts.size(), Double.POSITIVE_INFINITY);
            rights = bigArrays().resize(rights, tops.size());
            rights.fill(from, rights.size(), Double.NEGATIVE_INFINITY);
        }
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (owningBucketOrdinal >= tops.size()) {
            return buildEmptyAggregation();
        }
        double top = tops.get(owningBucketOrdinal);
        double bottom = bottoms.get(owningBucketOrdinal);
        double left = lefts.get(owningBucketOrdinal);
        double right = rights.get(owningBucketOrdinal);
        return new InternalCartesianBounds(name, top, bottom, left, right, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return InternalCartesianBounds.empty(name, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(tops, bottoms, lefts, rights);
    }
}
