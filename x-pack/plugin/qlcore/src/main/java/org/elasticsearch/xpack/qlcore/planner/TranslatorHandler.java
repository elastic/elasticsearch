/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.qlcore.planner;

import org.elasticsearch.xpack.qlcore.expression.Expression;
import org.elasticsearch.xpack.qlcore.expression.FieldAttribute;
import org.elasticsearch.xpack.qlcore.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.qlcore.querydsl.query.Query;
import org.elasticsearch.xpack.qlcore.querydsl.query.ScriptQuery;
import org.elasticsearch.xpack.qlcore.type.DataType;

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
            return ExpressionTranslator.wrapIfNested(querySupplier.get(), field);
        }
        return new ScriptQuery(sf.source(), sf.asScript());
    }

    String nameOf(Expression e);

    Object convert(Object value, DataType dataType);
}
