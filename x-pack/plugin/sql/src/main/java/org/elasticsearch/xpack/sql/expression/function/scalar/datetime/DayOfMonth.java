/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo.NodeCtor2;

import java.time.ZoneId;

/**
 * Extract the day of the month from a datetime.
 */
public class DayOfMonth extends DateTimeFunction {
    public DayOfMonth(Source source, Expression field, ZoneId zoneId) {
        super(source, field, zoneId, DateTimeExtractor.DAY_OF_MONTH);
    }

    @Override
    protected NodeCtor2<Expression, ZoneId, BaseDateTimeFunction> ctorForInfo() {
        return DayOfMonth::new;
    }

    @Override
    protected DayOfMonth replaceChild(Expression newChild) {
        return new DayOfMonth(source(), newChild, zoneId());
    }

    @Override
    public String dateTimeFormat() {
        return "d";
    }
}
