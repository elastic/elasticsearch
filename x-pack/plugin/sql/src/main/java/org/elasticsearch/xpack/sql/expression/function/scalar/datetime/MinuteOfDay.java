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
 * Extract the minute of the day from a datetime.
 */
public class MinuteOfDay extends DateTimeFunction {

    public MinuteOfDay(Location location, Expression field, TimeZone timeZone) {
        super(location, field, timeZone, DateTimeExtractor.MINUTE_OF_DAY);
    }

    @Override
    protected NodeCtor2<Expression, TimeZone, BaseDateTimeFunction> ctorForInfo() {
        return MinuteOfDay::new;
    }

    @Override
    protected MinuteOfDay replaceChild(Expression newChild) {
        return new MinuteOfDay(location(), newChild, timeZone());
    }

    @Override
    public String dateTimeFormat() {
        throw new UnsupportedOperationException("is there a format for it?");
    }
}
