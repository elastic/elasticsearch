/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingCountAggregator;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;

/**
 * Basic class that handles the translation of logical aggregate provider to the compute agg provider.
 * Its purpose is to encapsulate the various low-level details for each aggregate provider (which could be placed inside the aggregate
 * provider implementation itself).
 */
// NOTE: this would look even better with JEP 406 & co
class AggregateMapper {

    static AggregatorFunction.Factory map(AggregateFunction aggregateFunction) {
        if (aggregateFunction instanceof Avg avg) {
            return avg.field().dataType().isRational() ? AggregatorFunction.AVG_DOUBLES : AggregatorFunction.AVG_LONGS;
        }
        if (aggregateFunction instanceof Count) {
            return AggregatorFunction.COUNT;
        }
        if (aggregateFunction instanceof Max) {
            return aggregateFunction.field().dataType().isRational() ? AggregatorFunction.MAX_DOUBLES : AggregatorFunction.MAX_LONGS;
        }
        if (aggregateFunction instanceof Min) {
            return aggregateFunction.field().dataType().isRational() ? AggregatorFunction.MIN_DOUBLES : AggregatorFunction.MIN_LONGS;
        }
        if (aggregateFunction instanceof Sum) {
            return aggregateFunction.field().dataType().isRational() ? AggregatorFunction.SUM_DOUBLES : AggregatorFunction.SUM_LONGS;
        }
        throw new UnsupportedOperationException("No provider available for aggregate function=" + aggregateFunction);
    }

    static GroupingAggregatorFunction.Factory mapGrouping(AggregateFunction aggregateFunction) {
        GroupingAggregatorFunction.Factory aggregatorFunc;
        if (aggregateFunction instanceof Avg) {
            aggregatorFunc = aggregateFunction.field().dataType().isRational()
                ? GroupingAggregatorFunction.AVG_DOUBLES
                : GroupingAggregatorFunction.AVG_LONGS;
        } else if (aggregateFunction instanceof Count) {
            aggregatorFunc = GroupingAggregatorFunction.COUNT;
        } else if (aggregateFunction instanceof Max) {
            aggregatorFunc = aggregateFunction.field().dataType().isRational()
                ? GroupingAggregatorFunction.MAX_DOUBLES
                : GroupingCountAggregator.MAX_LONGS;
        } else if (aggregateFunction instanceof Min) {
            aggregatorFunc = aggregateFunction.field().dataType().isRational()
                ? GroupingAggregatorFunction.MIN_DOUBLES
                : GroupingAggregatorFunction.MIN_LONGS;
        } else if (aggregateFunction instanceof Sum) {
            aggregatorFunc = aggregateFunction.field().dataType().isRational()
                ? GroupingAggregatorFunction.SUM_DOUBLES
                : GroupingAggregatorFunction.SUM_LONGS;
        } else {
            throw new UnsupportedOperationException("unsupported aggregate function:" + aggregateFunction);
        }
        return aggregatorFunc;
    }
}
