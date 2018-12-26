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

import java.time.ZoneId;

/**
 * Exract the minute of the hour from a datetime.
 */
public class MinuteOfHour extends DateTimeFunction {
    public MinuteOfHour(Location location, Expression field, ZoneId zoneId) {
        super(location, field, zoneId, DateTimeExtractor.MINUTE_OF_HOUR);
    }

    @Override
    protected NodeCtor2<Expression, ZoneId, BaseDateTimeFunction> ctorForInfo() {
        return MinuteOfHour::new;
    }

    @Override
    protected MinuteOfHour replaceChild(Expression newChild) {
        return new MinuteOfHour(location(), newChild, zoneId());
    }

    @Override
    public String dateTimeFormat() {
        return "m";
    }
}
