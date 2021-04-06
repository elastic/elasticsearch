/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;

class CompositeAggregationFactory extends AggregatorFactory {
    private final int size;
    private final CompositeValuesSourceConfig[] sources;
    private final CompositeKey afterKey;

    CompositeAggregationFactory(String name, AggregationContext context, AggregatorFactory parent,
                                AggregatorFactories.Builder subFactoriesBuilder, Map<String, Object> metadata,
                                int size, CompositeValuesSourceConfig[] sources, CompositeKey afterKey) throws IOException {
        super(name, context, parent, subFactoriesBuilder, metadata);
        this.size = size;
        this.sources = sources;
        this.afterKey = afterKey;
    }

    @Override
    protected Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        return new CompositeAggregator(name, factories, context, parent, metadata, size, sources, afterKey);
    }
}
