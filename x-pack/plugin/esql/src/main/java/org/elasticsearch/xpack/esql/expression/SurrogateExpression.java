/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;

/**
 * Interface signaling to the planner that the declaring expression
 * has to be replaced by a different form.
 * Implement this on {@link AggregateFunction}s when either:
 * <ul>
 *     <li>The aggregation doesn't have a "native" implementation and instead
 *     should be replaced with a combination of aggregations and then
 *     "put back together" on output. Like {@code AVG = SUM / COUNT}.</li>
 *     <li>The aggregation is folded if it receives constant
 *     input. Like {@code MIN(1) == 1}.</li>
 * </ul>
 */
public interface SurrogateExpression {
    /**
     * Returns the expression to be replaced by or {@code null} if this cannot
     * be replaced.
     */
    Expression surrogate();

    /**
     * Like {@link #surrogate()}, for surrogates that may only be used if all the nodes that run the query support them:
     * {@code minTransportVersion} is the minimum version of those nodes, see
     * {@link org.elasticsearch.xpack.esql.session.Versioned}. Unlike a
     * {@link org.elasticsearch.xpack.esql.capabilities.TransportVersionAware} expression, which is replaced once the plan
     * has its shape, this picks the surrogate itself.
     */
    default Expression surrogate(TransportVersion minTransportVersion) {
        return surrogate();
    }
}
