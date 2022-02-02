/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.nested;

import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;

public class NestedAggregatorFactory extends AggregatorFactory {

    private final NestedObjectMapper parentObjectMapper;
    private final NestedObjectMapper childObjectMapper;

    NestedAggregatorFactory(
        String name,
        NestedObjectMapper parentObjectMapper,
        NestedObjectMapper childObjectMapper,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactories,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, subFactories, metadata);
        this.parentObjectMapper = parentObjectMapper;
        this.childObjectMapper = childObjectMapper;
    }

    @Override
    public Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        if (childObjectMapper == null) {
            return new Unmapped(name, context, parent, factories, metadata);
        }
        return new NestedAggregator(name, factories, parentObjectMapper, childObjectMapper, context, parent, cardinality, metadata);
    }

    private static final class Unmapped extends NonCollectingAggregator {

        Unmapped(String name, AggregationContext context, Aggregator parent, AggregatorFactories factories, Map<String, Object> metadata)
            throws IOException {
            super(name, context, parent, factories, metadata);
        }

        @Override
        public InternalAggregation buildEmptyAggregation() {
            return new InternalNested(name, 0, buildEmptySubAggregations(), metadata());
        }
    }

}
