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
import java.util.concurrent.TimeUnit;

/**
 * Extract the year from a datetime.
 */
public class Year extends DateTimeHistogramFunction {

    private static long YEAR_IN_MILLIS = TimeUnit.DAYS.toMillis(1) * 365L;

    public Year(Source source, Expression field, ZoneId zoneId) {
        super(source, field, zoneId, DateTimeExtractor.YEAR);
    }

    @Override
    protected NodeCtor2<Expression, ZoneId, BaseDateTimeFunction> ctorForInfo() {
        return Year::new;
    }

    @Override
    protected Year replaceChild(Expression newChild) {
        return new Year(source(), newChild, zoneId());
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
