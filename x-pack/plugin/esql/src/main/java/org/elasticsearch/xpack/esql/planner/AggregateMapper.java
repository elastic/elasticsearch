/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.AggregatorFunctionProviders;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;

import static org.elasticsearch.compute.aggregation.AggregatorFunctionProviders.avgDouble;
import static org.elasticsearch.compute.aggregation.AggregatorFunctionProviders.avgLong;

/**
 * Basic class that handles the translation of logical aggregate provider to the compute agg provider.
 * Its purpose is to encapsulate the various low-level details for each aggregate provider (which could be placed inside the aggregate
 * provider implementation itself).
 */
class AggregateMapper {

    static AggregatorFunction.Provider map(AggregateFunction aggregateFunction) {
        if (aggregateFunction instanceof Avg avg) {
            return avg.dataType().isRational() ? avgDouble() : avgLong();
        }

        if (aggregateFunction instanceof Count) {
            return AggregatorFunctionProviders.count();
        }

        throw new UnsupportedOperationException("No provider available for aggregate function=" + aggregateFunction);
    }
}
