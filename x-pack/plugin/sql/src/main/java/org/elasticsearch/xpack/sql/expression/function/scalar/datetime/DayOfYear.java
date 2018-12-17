/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo.NodeCtor2;

import java.time.ZoneId;

/**
 * Extract the day of the year from a datetime.
 */
public class DayOfYear extends DateTimeFunction {
    public DayOfYear(Location location, Expression field, ZoneId zoneId) {
        super(location, field, zoneId, DateTimeExtractor.DAY_OF_YEAR);
    }

    @Override
    protected NodeCtor2<Expression, ZoneId, BaseDateTimeFunction> ctorForInfo() {
        return DayOfYear::new;
    }

    @Override
    protected UnaryScalarFunction replaceChild(Expression newChild) {
        return new DayOfYear(location(), newChild, zoneId());
    }

    @Override
    public String dateTimeFormat() {
        return "D";
    }
}
