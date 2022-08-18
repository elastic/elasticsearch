/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TimeSeriesAggregationAggregationFactory extends ValuesSourceAggregatorFactory {

    private final boolean keyed;
    private final List<String> group;
    private final List<String> without;
    private final DateHistogramInterval interval;
    private final DateHistogramInterval offset;
    private final org.elasticsearch.search.aggregations.timeseries.aggregation.Aggregator aggregator;
    private final Map<String, Object> aggregatorParams;
    private final Downsample downsample;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final long startTime;
    private final long endTime;
    private boolean deferring;
    private final BucketOrder order;
    private final ValuesSourceConfig config;
    private final TimeSeriesAggregationAggregatorSupplier aggregatorSupplier;

    public TimeSeriesAggregationAggregationFactory(
        String name,
        boolean keyed,
        List<String> group,
        List<String> without,
        DateHistogramInterval interval,
        DateHistogramInterval offset,
        org.elasticsearch.search.aggregations.timeseries.aggregation.Aggregator aggregator,
        Map<String, Object> aggregatorParams,
        Downsample downsample,
        TermsAggregator.BucketCountThresholds bucketCountThresholds,
        BucketOrder order,
        long startTime,
        long endTime,
        boolean deferring,
        ValuesSourceConfig config,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        TimeSeriesAggregationAggregatorSupplier aggregatorSupplier
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);
        this.keyed = keyed;
        this.group = group;
        this.without = without;
        this.interval = interval;
        this.offset = offset;
        this.aggregator = aggregator;
        this.aggregatorParams = aggregatorParams;
        this.downsample = downsample;
        this.bucketCountThresholds = bucketCountThresholds;
        this.startTime = startTime;
        this.endTime = endTime;
        this.deferring = deferring;
        this.order = order;
        this.config = config;
        this.aggregatorSupplier = aggregatorSupplier;
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            TimeSeriesAggregationAggregationBuilder.REGISTRY_KEY,
            List.of(CoreValuesSourceType.NUMERIC),
            TimeSeriesAggregationAggregator::new,
            true
        );
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(this.bucketCountThresholds);
        if (InternalOrder.isKeyOrder(order) == false
            && thresholds.getShardSize() == TimeSeriesAggregationAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS.getShardSize()) {
            // The user has not made a shardSize selection. Use default
            // heuristic to avoid any wrong-ranking caused by distributed
            // counting
            thresholds.setShardSize(BucketUtils.suggestShardSideQueueSize(thresholds.getRequiredSize()));
        }
        thresholds.ensureValidity();
        return new TimeSeriesAggregationAggregator(
            name,
            factories,
            keyed,
            group,
            without,
            interval,
            offset,
            aggregator,
            aggregatorParams,
            downsample,
            thresholds,
            order,
            startTime,
            endTime,
            deferring,
            config,
            context,
            parent,
            CardinalityUpperBound.NONE,
            metadata
        );
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(this.bucketCountThresholds);
        if (InternalOrder.isKeyOrder(order) == false
            && thresholds.getShardSize() == TimeSeriesAggregationAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS.getShardSize()) {
            // The user has not made a shardSize selection. Use default
            // heuristic to avoid any wrong-ranking caused by distributed
            // counting
            thresholds.setShardSize(BucketUtils.suggestShardSideQueueSize(thresholds.getRequiredSize()));
        }
        thresholds.ensureValidity();
        return aggregatorSupplier.build(
            name,
            factories,
            keyed,
            group,
            without,
            interval,
            offset,
            aggregator,
            aggregatorParams,
            downsample,
            thresholds,
            order,
            startTime,
            endTime,
            deferring,
            config,
            context,
            parent,
            cardinality,
            metadata
        );
    }
}
