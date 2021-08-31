/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;

import java.io.IOException;
import java.util.Map;

public abstract class ValuesSourceAggregatorFactory extends AggregatorFactory {

    protected ValuesSourceConfig config;

    public ValuesSourceAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, subFactoriesBuilder, metadata);
        this.config = config;
    }

    @Override
    public Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        if (config.hasValues() == false) {
            return createUnmapped(parent, metadata);
        }
        return doCreateInternal(parent, cardinality, metadata);
    }

    /**
     * Create the {@linkplain Aggregator} for a {@link ValuesSource} that
     * doesn't have values.
     */
    protected abstract Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException;

    /**
     * Create the {@linkplain Aggregator} for a {@link ValuesSource} that has
     * values.
     *
     * @param cardinality Upper bound of the number of {@code owningBucketOrd}s
     *                    that the {@link Aggregator} created by this method
     *                    will be asked to collect.
     */
    protected abstract Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException;

    @Override
    public String getStatsSubtype() {
        return config.valueSourceType().typeName();
    }
}
