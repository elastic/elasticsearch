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
import java.util.concurrent.TimeUnit;

/**
 * Extract the year from a datetime.
 */
public class Year extends DateTimeHistogramFunction {

    private static long YEAR_IN_MILLIS = TimeUnit.DAYS.toMillis(1) * 365L;

    public Year(Location location, Expression field, TimeZone timeZone) {
        super(location, field, timeZone, DateTimeExtractor.YEAR);
    }

    @Override
    protected NodeCtor2<Expression, TimeZone, BaseDateTimeFunction> ctorForInfo() {
        return Year::new;
    }

    @Override
    protected Year replaceChild(Expression newChild) {
        return new Year(location(), newChild, timeZone());
    }

    @Override
    public String dateTimeFormat() {
        return "year";
    }

    @Override
    public Expression orderBy() {
        return field();
    }

    @Override
    public long interval() {
        return YEAR_IN_MILLIS;
    }
}
