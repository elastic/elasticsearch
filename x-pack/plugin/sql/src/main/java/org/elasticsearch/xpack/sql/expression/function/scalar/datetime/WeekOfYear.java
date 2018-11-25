/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo.NodeCtor2;

import java.util.TimeZone;

/**
 * Extract the week of the year from a datetime.
 */
public class WeekOfYear extends DateTimeFunction {
    public WeekOfYear(Location location, Expression field, TimeZone timeZone) {
        super(location, field, timeZone, DateTimeExtractor.WEEK_OF_YEAR);
    }

    @Override
    protected NodeCtor2<Expression, TimeZone, BaseDateTimeFunction> ctorForInfo() {
        return WeekOfYear::new;
    }

    @Override
    protected WeekOfYear replaceChild(Expression newChild) {
        return new WeekOfYear(location(), newChild, timeZone());
    }

    @Override
    public String dateTimeFormat() {
        return "w";
    }
}
