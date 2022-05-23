/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.function;

import java.util.Map;

import org.apache.lucene.util.hppc.BitMixer;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.elasticsearch.search.aggregations.metrics.InternalCardinality;
import org.elasticsearch.search.aggregations.timeseries.aggregation.TimePoint;

public class CountValuesFunction implements AggregatorFunction<TimePoint, Long> {

    private HyperLogLogPlusPlus counts;

    public CountValuesFunction() {
        counts = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.DEFAULT_PRECISION, BigArrays.NON_RECYCLING_INSTANCE, 1);
    }

    @Override
    public void collect(TimePoint value) {
        long hash = BitMixer.mix64(java.lang.Double.doubleToLongBits(value.getValue()));
        counts.collect(0, hash);
    }

    @Override
    public Long get() {
        AbstractHyperLogLogPlusPlus copy = counts.clone(0, BigArrays.NON_RECYCLING_INSTANCE);
        return copy.cardinality(0);
    }

    @Override
    public InternalAggregation getAggregation(
        DocValueFormat formatter, Map<String, Object> metadata) {
        AbstractHyperLogLogPlusPlus copy = counts.clone(0, BigArrays.NON_RECYCLING_INSTANCE);
        return new InternalCardinality(CardinalityAggregationBuilder.NAME, copy, metadata);
    }
}
