/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;
import org.elasticsearch.xpack.sql.tree.NodeInfo.NodeCtor2;
import org.elasticsearch.xpack.sql.tree.Source;

import java.time.ZoneId;

/**
 * Extract the hour of the day from a datetime.
 */
public class HourOfDay extends TimeFunction {
    public HourOfDay(Source source, Expression field, ZoneId zoneId) {
        super(source, field, zoneId, DateTimeExtractor.HOUR_OF_DAY);
    }

    @Override
    protected NodeCtor2<Expression, ZoneId, BaseDateTimeFunction> ctorForInfo() {
        return HourOfDay::new;
    }

    @Override
    protected HourOfDay replaceChild(Expression newChild) {
        return new HourOfDay(source(), newChild, zoneId());
    }

    @Override
    public String dateTimeFormat() {
        return "hour";
    }
}
