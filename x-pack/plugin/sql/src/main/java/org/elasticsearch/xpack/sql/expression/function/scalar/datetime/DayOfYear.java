/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.tree.NodeInfo.NodeCtor2;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;

import java.time.ZoneId;

/**
 * Extract the day of the year from a datetime.
 */
public class DayOfYear extends DateTimeFunction {
    public DayOfYear(Source source, Expression field, ZoneId zoneId) {
        super(source, field, zoneId, DateTimeExtractor.DAY_OF_YEAR);
    }

    @Override
    protected NodeCtor2<Expression, ZoneId, BaseDateTimeFunction> ctorForInfo() {
        return DayOfYear::new;
    }

    @Override
    protected UnaryScalarFunction replaceChild(Expression newChild) {
        return new DayOfYear(source(), newChild, zoneId());
    }

    @Override
    public String dateTimeFormat() {
        return "D";
    }
}
