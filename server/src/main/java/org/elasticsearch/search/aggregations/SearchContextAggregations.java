/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.apache.lucene.search.CollectorManager;

import java.util.function.Supplier;

/**
 * The aggregation context that is part of the search context.
 */
public class SearchContextAggregations {

    private final AggregatorFactories factories;
    private final Supplier<AggregationReduceContext.Builder> toAggregationReduceContextBuilder;
    private CollectorManager<AggregatorCollector, Void> aggCollectorManager;

    /**
     * Creates a new aggregation context with the parsed aggregator factories
     */
    public SearchContextAggregations(
        AggregatorFactories factories,
        Supplier<AggregationReduceContext.Builder> toAggregationReduceContextBuilder
    ) {
        this.factories = factories;
        this.toAggregationReduceContextBuilder = toAggregationReduceContextBuilder;
    }

    public AggregatorFactories factories() {
        return factories;
    }

    /**
     * Registers the collector to be run for the aggregations phase
     */
    public void registerAggsCollectorManager(CollectorManager<AggregatorCollector, Void> aggCollectorManager) {
        this.aggCollectorManager = aggCollectorManager;
    }

    /**
     * Returns the collector to be run for the aggregations phase
     */
    public CollectorManager<AggregatorCollector, Void> getAggsCollectorManager() {
        return aggCollectorManager;
    }

    /**
     * Returns if the aggregations needs to execute in sort order.
     */
    public boolean isInSortOrderExecutionRequired() {
        return factories.context() != null && factories.context().isInSortOrderExecutionRequired();
    }

    /**
     * Returns a builder for the reduce context.
     */
    public AggregationReduceContext.Builder getAggregationReduceContextBuilder() {
        return toAggregationReduceContextBuilder.get();
    }
}
