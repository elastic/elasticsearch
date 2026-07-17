/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;

/**
 * Marker interface for {@link TimeSeriesAggregateFunction}s to identify classes of functions that operate
 * on the temporality field of an index.
 * Implementations of this interface need to expect the temporality {@code Attribute} to be passed after all regular arguments.
 * Implementations that also need the {code @timestamp} should additionally implement {@link TimestampAware}.
 */
public interface TemporalityAware {

    /**
     * The current value for the temporality argument of the function.
     * If this is {@code null}, it will be injected automatically during local planning.
     * @return the current value for the temporality argument
     */
    @Nullable
    Expression temporality();

    /**
     * Returns a copy of this function with the provided value as temporality argument.
     */
    TimeSeriesAggregateFunction withTemporality(Expression temporality);
}
