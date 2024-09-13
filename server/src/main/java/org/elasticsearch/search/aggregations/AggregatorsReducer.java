/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.HashMap;
import java.util.Map;

/**
 *  Interface for reducing {@link InternalAggregations} to a single one in a streaming fashion.
 */
public final class AggregatorsReducer implements Releasable {

    private final Map<String, AggregatorReducer> aggByName;

    /**
     * Solo constructor
     *
     * @param proto The prototype {@link InternalAggregations} we are aggregating.
     * @param context The aggregation context
     * @param size The number of {@link InternalAggregations} we are aggregating.
     */
    public AggregatorsReducer(InternalAggregations proto, AggregationReduceContext context, int size) {
        aggByName = new HashMap<>(proto.asList().size());
        for (InternalAggregation aggregation : proto) {
            aggByName.put(aggregation.getName(), aggregation.getReducer(context.forAgg(aggregation.getName()), size));
        }
    }

    /**
     * Adds a {@link InternalAggregations} for reduction.
     */
    public void accept(InternalAggregations aggregations) {
        for (InternalAggregation aggregation : aggregations) {
            final AggregatorReducer reducer = aggByName.get(aggregation.getName());
            reducer.accept(aggregation);
        }
    }

    /**
     * returns the reduced {@link InternalAggregations}.
     */
    public InternalAggregations get() {
        return InternalAggregations.from(aggByName.values().stream().map(AggregatorReducer::get).toList());
    }

    @Override
    public void close() {
        Releasables.close(aggByName.values());
    }
}
