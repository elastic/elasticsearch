/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.global;

import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;

public class GlobalAggregatorFactory extends AggregatorFactory {

    public GlobalAggregatorFactory(String name,
                                    AggregationContext context,
                                    AggregatorFactory parent,
                                    AggregatorFactories.Builder subFactories,
                                    Map<String, Object> metadata) throws IOException {
        super(name, context, parent, subFactories, metadata);
    }

    @Override
    public Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        if (parent != null) {
            throw new AggregationExecutionException("Aggregation [" + parent.name() + "] cannot have a global " + "sub-aggregation [" + name
                    + "]. Global aggregations can only be defined as top level aggregations");
        }
        if (cardinality != CardinalityUpperBound.ONE) {
            throw new AggregationExecutionException("Aggregation [" + name() + "] must have cardinality 1 but was [" + cardinality + "]");
        }
        return new GlobalAggregator(name, factories, context, metadata);
    }
}
