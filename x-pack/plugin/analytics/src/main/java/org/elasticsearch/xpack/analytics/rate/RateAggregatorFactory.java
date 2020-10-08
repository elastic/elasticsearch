/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.rate;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType;

class RateAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final Rounding.DateTimeUnit rateUnit;

    RateAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        Rounding.DateTimeUnit rateUnit,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.rateUnit = rateUnit;
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
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new AbstractRateAggregator(name, config, rateUnit, searchContext, parent, metadata) {
            @Override
            public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) {
                return LeafBucketCollector.NO_OP_COLLECTOR;
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound bucketCardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return queryShardContext.getValuesSourceRegistry()
            .getAggregator(RateAggregationBuilder.REGISTRY_KEY, config)
            .build(name, config, rateUnit, searchContext, parent, metadata);
    }
}
