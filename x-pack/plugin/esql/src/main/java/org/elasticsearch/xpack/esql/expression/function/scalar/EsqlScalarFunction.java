/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;

import java.util.List;

/**
 * A {@code ScalarFunction} is a {@code Function} that takes values from some
 * operation and converts each to another value. An example would be
 * {@code ABS()}, which takes one value at a time, applies a function to the
 * value (abs) and returns a new value.
 * <p>
 *     We have a guide for writing these in the javadoc for
 *     {@link org.elasticsearch.xpack.esql.expression.function.scalar}.
 * </p>
 */
public abstract class EsqlScalarFunction extends ScalarFunction implements EvaluatorMapper {
    protected EsqlScalarFunction(Source source) {
        super(source);
    }

    protected EsqlScalarFunction(Source source, List<Expression> fields) {
        super(source, fields);
    }

    @Override
    public Object fold() {
        return EvaluatorMapper.super.fold();
    }
}
