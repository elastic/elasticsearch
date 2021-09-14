/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Unmapped;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.Map;

public class AbstractRangeAggregatorFactory<R extends Range> extends ValuesSourceAggregatorFactory {

    private final InternalRange.Factory<?, ?> rangeFactory;
    private final R[] ranges;
    private final boolean keyed;
    private final ValuesSourceRegistry.RegistryKey<RangeAggregatorSupplier> registryKey;
    private final RangeAggregatorSupplier aggregatorSupplier;

    public AbstractRangeAggregatorFactory(
        String name,
        ValuesSourceRegistry.RegistryKey<RangeAggregatorSupplier> registryKey,
        ValuesSourceConfig config,
        R[] ranges,
        boolean keyed,
        InternalRange.Factory<?, ?> rangeFactory,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        RangeAggregatorSupplier aggregatorSupplier
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);
        this.ranges = ranges;
        this.keyed = keyed;
        this.rangeFactory = rangeFactory;
        this.registryKey = registryKey;
        this.aggregatorSupplier = aggregatorSupplier;
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new Unmapped<>(name, factories, ranges, keyed, config.format(), context, parent, rangeFactory, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        return aggregatorSupplier.build(name, factories, config, rangeFactory, ranges, keyed, context, parent, cardinality, metadata);
    }
}
