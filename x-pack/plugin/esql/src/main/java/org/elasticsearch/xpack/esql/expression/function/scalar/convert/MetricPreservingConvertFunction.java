/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.xpack.esql.core.expression.MetricExpression;

/**
 * A numeric or histogram representation conversion that preserves metric identity without declaring a non-metric input to be a metric.
 * Other conversions and value transformations do not inherit metric identity.
 */
public interface MetricPreservingConvertFunction extends ConvertFunction, MetricExpression {
    @Override
    default boolean isMetric() {
        return MetricExpression.isMetric(field());
    }
}
