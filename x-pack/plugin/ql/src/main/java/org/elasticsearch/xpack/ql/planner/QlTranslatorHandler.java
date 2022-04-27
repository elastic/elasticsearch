/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.planner;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypeConverter;

public class QlTranslatorHandler implements TranslatorHandler {

    @Override
    public Query asQuery(Expression e) {
        return ExpressionTranslators.toQuery(e, this);
    }

    @Override
    public String nameOf(Expression e) {
        if (e instanceof NamedExpression) {
            return ((NamedExpression) e).name();
        } else {
            return e.sourceText();
        }
    }

    @Override
    public Object convert(Object value, DataType dataType) {
        return DataTypeConverter.convert(value, dataType);
    }
}
