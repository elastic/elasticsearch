/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;

/**
 * Marker interface for {@link TimeSeriesAggregateFunction}s to identify classes of functions that operate
 * on the {code @timestamp} and the temporality field of an index.
 * Implementations of this interface need to expect the associated {@code Attribute}s to be passed after all regular arguments.
 * The {code @timestamp} will be passed first, followed by the temporality.
 */
public interface TemporalityAware extends TimestampAware {

    Expression temporality();

    TimeSeriesAggregateFunction withTemporality(Expression injectedTemporality);
}
