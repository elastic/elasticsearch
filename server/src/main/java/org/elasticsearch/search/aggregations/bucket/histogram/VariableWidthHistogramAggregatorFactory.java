/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.Map;

public class VariableWidthHistogramAggregatorFactory extends ValuesSourceAggregatorFactory {

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            VariableWidthHistogramAggregationBuilder.REGISTRY_KEY,
            CoreValuesSourceType.NUMERIC,
            VariableWidthHistogramAggregator::new,
                true);
    }

    private final VariableWidthHistogramAggregatorSupplier aggregatorSupplier;
    private final int numBuckets;
    private final int shardSize;
    private final int initialBuffer;

    VariableWidthHistogramAggregatorFactory(String name,
                                            ValuesSourceConfig config,
                                            int numBuckets,
                                            int shardSize,
                                            int initialBuffer,
                                            AggregationContext context,
                                            AggregatorFactory parent,
                                            AggregatorFactories.Builder subFactoriesBuilder,
                                            Map<String, Object> metadata,
                                            VariableWidthHistogramAggregatorSupplier aggregatorSupplier) throws IOException{
        super(name, config, context, parent, subFactoriesBuilder, metadata);
        this.aggregatorSupplier = aggregatorSupplier;
        this.numBuckets = numBuckets;
        this.shardSize = shardSize;
        this.initialBuffer = initialBuffer;
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        if (cardinality != CardinalityUpperBound.ONE) {
            throw new IllegalArgumentException(
                "["
                    + VariableWidthHistogramAggregationBuilder.NAME
                    + "] cannot be nested inside an aggregation that collects more than a single bucket."
            );
        }
        return aggregatorSupplier
            .build(name, factories, numBuckets, shardSize, initialBuffer, config, context, parent, metadata);
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new VariableWidthHistogramAggregator(name, factories, numBuckets, shardSize, initialBuffer, config,
            context, parent, metadata);
    }
}
