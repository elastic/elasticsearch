/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction;

import org.apache.lucene.util.hppc.BitMixer;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.elasticsearch.search.aggregations.metrics.InternalCardinality;

import java.util.Map;

public class CountValuesBucketFunction implements AggregatorBucketFunction<Double> {

    private HyperLogLogPlusPlus counts;

    public CountValuesBucketFunction(BigArrays bigArrays) {
        counts = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.DEFAULT_PRECISION, bigArrays, 1);
    }

    @Override
    public String name() {
        return "count_values";
    }

    @Override
    public void collect(Double number, long bucket) {
        long value = BitMixer.mix64(java.lang.Double.doubleToLongBits(number));
        counts.collect(bucket, value);
    }

    @Override
    public InternalAggregation getAggregation(
        long bucket,
        Map<String, Object> aggregatorParams,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        AbstractHyperLogLogPlusPlus copy = counts.clone(bucket, BigArrays.NON_RECYCLING_INSTANCE);
        return new InternalCardinality(CardinalityAggregationBuilder.NAME, copy, metadata);
    }

    @Override
    public void close() {
        Releasables.close(counts);
    }
}
