/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.planner;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.querydsl.query.ScriptQuery;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.function.Function;

/**
 * Parameterized handler used during query translation.
 *
 * Provides contextual utilities for an individual query to be performed.
 */
public interface TranslatorHandler {

    Query asQuery(Expression e);

    default Query wrapFunctionQuery(ScalarFunction sf, Expression field, Function<FieldAttribute, Query> querySupplier) {
        if (field instanceof FieldAttribute) {
            return ExpressionTranslator.wrapIfNested(querySupplier.apply((FieldAttribute) field), field);
        }
        return new ScriptQuery(sf.source(), sf.asScript());
    }

    String nameOf(Expression e);

    Object convert(Object value, DataType dataType);
}
