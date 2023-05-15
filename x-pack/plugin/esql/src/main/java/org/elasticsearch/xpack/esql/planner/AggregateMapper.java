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
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Locale;

/**
 * Basic class that handles the translation of logical aggregate provider to the compute agg provider.
 */
class AggregateMapper {

    static AggregationType mapToType(AggregateFunction aggregateFunction) {
        if (aggregateFunction.field().dataType() == DataTypes.LONG) {
            return AggregationType.longs;
        }
        if (aggregateFunction.field().dataType() == DataTypes.INTEGER) {
            return AggregationType.ints;
        }
        if (aggregateFunction.field().dataType() == DataTypes.DOUBLE) {
            return AggregationType.doubles;
        }
        if (aggregateFunction.field().dataType() == DataTypes.BOOLEAN) {
            return AggregationType.booleans;
        }
        if (aggregateFunction.field().dataType() == DataTypes.KEYWORD) {
            return AggregationType.bytesrefs;
        }
        if (aggregateFunction.field().dataType() == DataTypes.IP) {
            return AggregationType.bytesrefs;
        }
        if (aggregateFunction.field().dataType() == DataTypes.DATETIME) {
            return AggregationType.longs;
        }
        // agnostic here means "only works if the aggregation doesn't care about type".
        return AggregationType.agnostic;
    }

    static AggregationName mapToName(AggregateFunction aggregateFunction) {
        return AggregationName.of(aggregateFunction.functionName().toLowerCase(Locale.ROOT));
    }
}
