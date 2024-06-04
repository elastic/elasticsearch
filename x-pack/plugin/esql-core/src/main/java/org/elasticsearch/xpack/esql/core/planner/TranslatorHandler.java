/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.planner;

import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.function.Supplier;

/**
 * Parameterized handler used during query translation.
 *
 * Provides contextual utilities for an individual query to be performed.
 */
public interface TranslatorHandler {

    Query asQuery(Expression e);

    default Query wrapFunctionQuery(ScalarFunction sf, Expression field, Supplier<Query> querySupplier) {
        if (field instanceof FieldAttribute) {
            return querySupplier.get();
        }
        throw new QlIllegalArgumentException("Cannot translate expression:[" + sf.sourceText() + "]");
    }

    String nameOf(Expression e);

    Object convert(Object value, DataType dataType);
}
