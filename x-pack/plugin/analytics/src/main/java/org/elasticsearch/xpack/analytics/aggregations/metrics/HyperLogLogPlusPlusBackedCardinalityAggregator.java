/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus;
import org.elasticsearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.elasticsearch.search.aggregations.metrics.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xpack.analytics.aggregations.support.HyperLogLogPlusPlusValuesSource.HyperLogLogPlusPlusSketch;
import org.elasticsearch.xpack.analytics.mapper.fielddata.HyperLogLogPlusPlusValue;
import org.elasticsearch.xpack.analytics.mapper.fielddata.HyperLogLogPlusPlusValues;

import java.io.IOException;
import java.util.Map;

/**
 * An aggregator that computes approximate counts of unique values from HyperLogLogPlusPlus doc values.
 */
public class HyperLogLogPlusPlusBackedCardinalityAggregator extends NumericMetricsAggregator.SingleValue {

    private final HyperLogLogPlusPlusSketch valuesSource;
    private final HyperLogLogPlusPlus counts;

    public HyperLogLogPlusPlusBackedCardinalityAggregator(
            String name,
            ValuesSourceConfig valuesSourceConfig,
            int precision,
            SearchContext context,
            Aggregator parent,
            Map<String, Object> metadata) throws IOException {
        super(name, context, parent, metadata);
        assert valuesSourceConfig.hasValues(); // should always have doc values
        this.valuesSource = (HyperLogLogPlusPlusSketch) valuesSourceConfig.getValuesSource();
        this.counts = new HyperLogLogPlusPlus(precision, context.bigArrays(), 1);
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                               LeafBucketCollector sub) throws IOException {
        final HyperLogLogPlusPlusValues values = valuesSource.getHyperLogLogPlusPlusValues(ctx);
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    final HyperLogLogPlusPlusValue value = values.hllValue();
                    if (value.getAlgorithm() == HyperLogLogPlusPlusValue.Algorithm.HYPERLOGLOG) {
                        counts.merge(owningBucketOrd, value.getHyperLogLog());
                    } else {
                        counts.merge(owningBucketOrd, value.getLinearCounting());
                    }
                }
            }
        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        return counts.cardinality(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (owningBucketOrdinal >= counts.maxOrd() || counts.cardinality(owningBucketOrdinal) == 0) {
            return buildEmptyAggregation();
        }
        // We need to build a copy because the returned Aggregation needs remain usable after
        // this Aggregator (and its HLL++ counters) is released.
        AbstractHyperLogLogPlusPlus copy = counts.clone(owningBucketOrdinal, BigArrays.NON_RECYCLING_INSTANCE);
        return new InternalCardinality(name, copy, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalCardinality(name, null, metadata());
    }

    @Override
    protected void doClose() {
        Releasables.close(counts);
    }
}
