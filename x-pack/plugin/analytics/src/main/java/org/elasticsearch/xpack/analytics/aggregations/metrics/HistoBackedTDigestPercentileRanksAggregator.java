/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.aggregations.metrics;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

public class HistoBackedTDigestPercentileRanksAggregator extends AbstractHistoBackedTDigestPercentilesAggregator {

    public HistoBackedTDigestPercentileRanksAggregator(
        String name,
        ValuesSourceConfig config,
        AggregationContext context,
        Aggregator parent,
        double[] percents,
        double compression,
        String executionHint,
        boolean keyed,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, config, context, parent, percents, compression, executionHint, keyed, formatter, metadata);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        TDigestState state = getState(owningBucketOrdinal);
        if (state == null) {
            return buildEmptyAggregation();
        } else {
            return new InternalTDigestPercentileRanks(name, keys, state, keyed, formatter, metadata());
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return InternalTDigestPercentileRanks.empty(name, keys, compression, executionHint, keyed, formatter, metadata());
    }

    @Override
    public double metric(String name, long bucketOrd) {
        TDigestState state = getState(bucketOrd);
        if (state == null) {
            return Double.NaN;
        } else {
            return InternalTDigestPercentileRanks.percentileRank(state, Double.valueOf(name));
        }
    }
}
