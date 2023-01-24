/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.aggregation.AggregationName;
import org.elasticsearch.compute.aggregation.AggregationType;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;

import java.util.Locale;

/**
 * Basic class that handles the translation of logical aggregate provider to the compute agg provider.
 */
class AggregateMapper {

    static AggregationType mapToType(AggregateFunction aggregateFunction) {
        return aggregateFunction.field().dataType().isRational() ? AggregationType.doubles : AggregationType.longs;
    }

    static AggregationName mapToName(AggregateFunction aggregateFunction) {
        return AggregationName.of(aggregateFunction.functionName().toLowerCase(Locale.ROOT));
    }
}
