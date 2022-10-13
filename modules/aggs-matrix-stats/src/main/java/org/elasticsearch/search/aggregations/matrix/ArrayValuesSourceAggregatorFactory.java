/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.matrix;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class ArrayValuesSourceAggregatorFactory extends AggregatorFactory {

    protected Map<String, ValuesSourceConfig> configs;

    public ArrayValuesSourceAggregatorFactory(
        String name,
        Map<String, ValuesSourceConfig> configs,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, subFactoriesBuilder, metadata);
        this.configs = configs;
    }

    @Override
    public Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        HashMap<String, ValuesSource> valuesSources = new HashMap<>();

        for (Map.Entry<String, ValuesSourceConfig> config : configs.entrySet()) {
            ValuesSourceConfig vsc = config.getValue();
            if (vsc.hasValues()) {
                valuesSources.put(config.getKey(), vsc.getValuesSource());
            }
        }
        if (valuesSources.isEmpty()) {
            return createUnmapped(parent, metadata);
        }
        return doCreateInternal(valuesSources, parent, cardinality, metadata);
    }

    /**
     * Create the {@linkplain Aggregator} when none of the configured
     * fields can be resolved to a {@link ValuesSource}.
     */
    protected abstract Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException;

    /**
     * Create the {@linkplain Aggregator} when any of the configured
     * fields can be resolved to a {@link ValuesSource}.
     *
     * @param cardinality Upper bound of the number of {@code owningBucketOrd}s
     *                    that the {@link Aggregator} created by this method
     *                    will be asked to collect.
     */
    protected abstract Aggregator doCreateInternal(
        Map<String, ValuesSource> valuesSources,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException;

}
