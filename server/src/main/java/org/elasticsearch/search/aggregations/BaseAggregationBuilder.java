/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.xcontent.XContentParser;

import java.util.Map;

/**
 * Interface shared by {@link AggregationBuilder} and {@link PipelineAggregationBuilder} so they can conveniently share the same namespace
 * for {@link XContentParser#namedObject(Class, String, Object)}.
 */
public interface BaseAggregationBuilder {
    /**
     * The name of the type of aggregation built by this builder.
     */
    String getType();

    /**
     * Set the aggregation's metadata. Returns {@code this} for chaining.
     */
    BaseAggregationBuilder setMetadata(Map<String, Object> metadata);

    /**
     * Set the sub aggregations if this aggregation supports sub aggregations. Returns {@code this} for chaining.
     */
    BaseAggregationBuilder subAggregations(Builder subFactories);
}
