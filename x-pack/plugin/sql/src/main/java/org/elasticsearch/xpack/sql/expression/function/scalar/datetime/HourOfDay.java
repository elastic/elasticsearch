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

import java.time.temporal.ChronoField;
import java.util.TimeZone;

/**
 * Extract the hour of the day from a datetime.
 */
public class HourOfDay extends DateTimeFunction {
    public HourOfDay(Location location, Expression field, TimeZone timeZone) {
        super(location, field, timeZone);
    }

    @Override
    protected NodeCtor2<Expression, TimeZone, DateTimeFunction> ctorForInfo() {
        return HourOfDay::new;
    }

    @Override
    protected HourOfDay replaceChild(Expression newChild) {
        return new HourOfDay(location(), newChild, timeZone());
    }

    @Override
    public String dateTimeFormat() {
        return "hour";
    }

    @Override
    protected ChronoField chronoField() {
        return ChronoField.HOUR_OF_DAY;
    }

    @Override
    protected DateTimeExtractor extractor() {
        return DateTimeExtractor.HOUR_OF_DAY;
    }
}
