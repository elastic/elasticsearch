/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.planner.ExpressionTranslator;
import org.elasticsearch.xpack.ql.planner.TranslatorHandler;
import org.elasticsearch.xpack.ql.querydsl.query.GeoDistanceQuery;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.querydsl.query.ScriptQuery;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.StDistance;
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
    public Query wrapFunctionQuery(ScalarFunction sf, Expression field, Query q) {
        if (field instanceof StDistance && q instanceof GeoDistanceQuery) {
            return ExpressionTranslator.wrapIfNested(q, ((StDistance) field).left());
        }
        if (field instanceof FieldAttribute) {
            return ExpressionTranslator.wrapIfNested(q, field);
        }
        return new ScriptQuery(sf.source(), sf.asScript());
    }

    @Override
    public String nameOf(Expression e) {
        return QueryTranslator.nameOf(e);
    }

    @Override
    public String dateFormat(Expression e) {
        if (e instanceof DateTimeFunction) {
            return ((DateTimeFunction) e).dateTimeFormat();
        }
        return null;
    }

    @Override
    public Object convert(Object value, DataType dataType) {
        return SqlDataTypeConverter.convert(value, dataType);
    }
}
