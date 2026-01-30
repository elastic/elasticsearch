/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  Interface for reducing {@link InternalAggregations} to a single one in a streaming fashion.
 */
public final class AggregatorsReducer implements Releasable {

    private final Map<String, AggregatorReducer> aggByName;
    private final AggregationReduceContext context;
    private final int size;

    /**
     * Solo constructor
     *
     * @param proto The prototype {@link InternalAggregations} we are aggregating.
     * @param context The aggregation context
     * @param size The number of {@link InternalAggregations} we are aggregating.
     */
    public AggregatorsReducer(InternalAggregations proto, AggregationReduceContext context, int size) {
        this.context = context;
        this.size = size;
        aggByName = new HashMap<>(proto.asList().size());
    }

    /**
     * Adds a {@link InternalAggregations} for reduction.
     */
    public void accept(InternalAggregations aggregations) {
        for (InternalAggregation aggregation : aggregations) {
            AggregatorReducer reducer = aggByName.computeIfAbsent(
                aggregation.getName(),
                k -> aggregation.getReducer(context.forAgg(aggregation.getName()), size)
            );
            reducer.accept(aggregation);
        }
    }

    /**
     * returns the reduced {@link InternalAggregations}.
     */
    public InternalAggregations get() {
        final Collection<AggregatorReducer> reducers = aggByName.values();
        final List<InternalAggregation> aggs = new ArrayList<>(reducers.size());
        for (AggregatorReducer reducer : reducers) {
            aggs.add(reducer.get());
        }
        return InternalAggregations.from(aggs);
    }

    @Override
    public void close() {
        Releasables.close(aggByName.values());
    }
}
