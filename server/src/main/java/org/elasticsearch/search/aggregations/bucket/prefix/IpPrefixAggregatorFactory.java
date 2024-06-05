/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.prefix;

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

public class IpPrefixAggregatorFactory extends ValuesSourceAggregatorFactory {
    private final boolean keyed;
    private final long minDocCount;
    private final IpPrefixAggregator.IpPrefix ipPrefix;
    private final IpPrefixAggregationSupplier aggregationSupplier;

    public IpPrefixAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        boolean keyed,
        long minDocCount,
        IpPrefixAggregator.IpPrefix ipPrefix,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        IpPrefixAggregationSupplier aggregationSupplier
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.ipPrefix = ipPrefix;
        this.aggregationSupplier = aggregationSupplier;
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(IpPrefixAggregationBuilder.REGISTRY_KEY, CoreValuesSourceType.IP, IpPrefixAggregator::new, true);
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new IpPrefixAggregator.Unmapped(name, factories, config, keyed, minDocCount, context, parent, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        return aggregationSupplier.build(name, factories, config, keyed, minDocCount, ipPrefix, context, parent, cardinality, metadata);
    }
}
