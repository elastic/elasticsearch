/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.planner.TranslatorHandler;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.type.SqlDataTypeConverter;

public class SqlTranslatorHandler implements TranslatorHandler {

    private final boolean onAggs;

    public SqlTranslatorHandler(boolean onAggs) {
        this.onAggs = onAggs;
    }

    @Override
    public Query asQuery(Expression e) {
        return QueryTranslator.toQuery(e, onAggs).query;
    }

    @Override
    public String nameOf(Expression e) {
        return QueryTranslator.nameOf(e);
    }

    @Override
    public Object convert(Object value, DataType dataType) {
        return SqlDataTypeConverter.convert(value, dataType);
    }
}
