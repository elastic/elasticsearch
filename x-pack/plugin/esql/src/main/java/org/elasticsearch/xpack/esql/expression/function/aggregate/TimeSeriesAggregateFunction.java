/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;

/**
 * Extends {@link AggregateFunction} to support aggregation per time_series,
 * such as {@link Rate} or {@link MaxOverTime}.
 */
public abstract class TimeSeriesAggregateFunction extends AggregateFunction {

    protected TimeSeriesAggregateFunction(Source source, Expression field, Expression filter, List<? extends Expression> parameters) {
        super(source, field, filter, parameters);
    }

    protected TimeSeriesAggregateFunction(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Returns the aggregation function to be used in the first aggregation stage,
     * which is grouped by `_tsid` (and `time_bucket`).
     *
     * @see org.elasticsearch.xpack.esql.optimizer.rules.logical.TranslateTimeSeriesAggregate
     */
    public abstract AggregateFunction perTimeSeriesAggregation();
}
