/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.rate;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.TimeSeriesValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

class RateAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final RateAggregatorSupplier aggregatorSupplier;

    private final Rounding.DateTimeUnit rateUnit;

    private final RateMode rateMode;

    RateAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        Rounding.DateTimeUnit rateUnit,
        RateMode rateMode,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        RateAggregatorSupplier aggregatorSupplier
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);

        this.aggregatorSupplier = aggregatorSupplier;
        this.rateUnit = rateUnit;
        this.rateMode = rateMode;
    }

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            RateAggregationBuilder.REGISTRY_KEY,
            Collections.singletonList(CoreValuesSourceType.NUMERIC),
            NumericRateAggregator::new,
            true
        );
        builder.register(
            RateAggregationBuilder.REGISTRY_KEY,
            Collections.singletonList(AnalyticsValuesSourceType.HISTOGRAM),
            HistogramRateAggregator::new,
            true
        );
        builder.register(
            RateAggregationBuilder.REGISTRY_KEY,
            Collections.singletonList(TimeSeriesValuesSourceType.COUNTER),
            TimeSeriesRateAggregator::new,
            true
        );
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new AbstractRateAggregator(name, config, rateUnit, rateMode, context, parent, metadata) {
            @Override
            public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) {
                return LeafBucketCollector.NO_OP_COLLECTOR;
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound bucketCardinality, Map<String, Object> metadata)
        throws IOException {
        return aggregatorSupplier.build(name, config, rateUnit, rateMode, context, parent, metadata);
    }
}
