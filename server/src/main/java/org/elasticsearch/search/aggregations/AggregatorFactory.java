/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.SamplingContext;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.search.aggregations.support.AggregationUsageService.OTHER_SUBTYPE;

public abstract class AggregatorFactory {
    protected final String name;
    protected final AggregatorFactory parent;
    protected final AggregatorFactories factories;
    protected final Map<String, Object> metadata;

    protected final AggregationContext context;

    /**
     * Constructs a new aggregator factory.
     *
     * @param name
     *            The aggregation name
     * @throws IOException
     *             if an error occurs creating the factory
     */
    @SuppressWarnings("this-escape")
    public AggregatorFactory(
        String name,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        this.name = name;
        this.context = context;
        this.parent = parent;
        this.factories = subFactoriesBuilder.build(context, this);
        this.metadata = metadata;
    }

    /**
     * Climbs up the aggregation factory tree to find the sampling context if one exists.
     * @return Optional SamplingContext
     */
    public Optional<SamplingContext> getSamplingContext() {
        if (parent != null) {
            return parent.getSamplingContext();
        } else {
            return Optional.empty();
        }
    }

    public String name() {
        return name;
    }

    protected abstract Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException;

    /**
     * Creates the aggregator.
     *
     * @param parent The parent aggregator (if this is a top level factory, the
     *               parent will be {@code null})
     * @param cardinality Upper bound of the number of {@code owningBucketOrd}s
     *                    that the {@link Aggregator} created by this method
     *                    will be asked to collect.
     */
    public final Aggregator create(Aggregator parent, CardinalityUpperBound cardinality) throws IOException {
        return createInternal(parent, cardinality, this.metadata);
    }

    public AggregatorFactory getParent() {
        return parent;
    }

    /**
     * Returns the aggregation subtype for nodes usage stats.
     * <p>
     * It should match the types registered by calling {@linkplain org.elasticsearch.search.aggregations.support.AggregationUsageService}.
     * In other words, it should be ValueSourcesType for the VST aggregations OTHER_SUBTYPE for all other aggregations.
     */
    public String getStatsSubtype() {
        return OTHER_SUBTYPE;
    }
}
