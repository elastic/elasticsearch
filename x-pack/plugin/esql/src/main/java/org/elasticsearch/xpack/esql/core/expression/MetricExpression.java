/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

/**
 * An expression that represents a mapped time-series metric. This property is independent of counter semantics,
 * which are encoded in the data type: a cast from a counter to a plain numeric type can still produce a metric.
 */
public interface MetricExpression {
    /**
     * Whether this expression represents a mapped metric or preserves its input's metric identity.
     */
    boolean isMetric();

    /**
     * Whether the expression carries metric identity; other computations do not inherit it from their inputs.
     */
    static boolean isMetric(Expression expression) {
        return expression instanceof MetricExpression metric && metric.isMetric();
    }
}
