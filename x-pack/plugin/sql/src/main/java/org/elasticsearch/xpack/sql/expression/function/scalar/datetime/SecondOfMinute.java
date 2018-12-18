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
 * Extract the second of the minute from a datetime.
 */
public class SecondOfMinute extends DateTimeFunction {
    public SecondOfMinute(Location location, Expression field, ZoneId zoneId) {
        super(location, field, zoneId, DateTimeExtractor.SECOND_OF_MINUTE);
    }

    @Override
    protected NodeCtor2<Expression, ZoneId, BaseDateTimeFunction> ctorForInfo() {
        return SecondOfMinute::new;
    }

    @Override
    protected SecondOfMinute replaceChild(Expression newChild) {
        return new SecondOfMinute(location(), newChild, zoneId());
    }

    @Override
    public String dateTimeFormat() {
        return "s";
    }
}
