/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;

import java.io.IOException;
import java.util.Map;

public abstract class MultiValuesSourceAggregatorFactory extends AggregatorFactory {

    protected final Map<String, ValuesSourceConfig> configs;
    protected final DocValueFormat format;

    public MultiValuesSourceAggregatorFactory(String name, Map<String, ValuesSourceConfig> configs,
                                              DocValueFormat format, AggregationContext context,
                                              AggregatorFactory parent, AggregatorFactories.Builder subFactoriesBuilder,
                                              Map<String, Object> metadata) throws IOException {
        super(name, context, parent, subFactoriesBuilder, metadata);
        this.configs = configs;
        this.format = format;
    }

    @Override
    public Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        return doCreateInternal(configs, format, parent, cardinality, metadata);
    }

    /**
     * Create an aggregator that won't collect anything but will return an
     * appropriate empty aggregation.
     */
    protected abstract Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException;

    /**
     * Create the {@linkplain Aggregator}.
     *
     * @param cardinality Upper bound of the number of {@code owningBucketOrd}s
     *                    that the {@link Aggregator} created by this method
     *                    will be asked to collect.
     */
    protected abstract Aggregator doCreateInternal(
        Map<String, ValuesSourceConfig> configs,
        DocValueFormat format,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException;

}
