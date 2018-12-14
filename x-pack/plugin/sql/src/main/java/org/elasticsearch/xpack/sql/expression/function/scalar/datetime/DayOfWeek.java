/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.NonIsoDateTimeProcessor.NonIsoDateTimeExtractor;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo.NodeCtor2;

import java.util.TimeZone;

/**
 * Extract the day of the week from a datetime in non-ISO format. 1 is Sunday, 2 is Monday, etc.
 */
public class DayOfWeek extends NonIsoDateTimeFunction {
    
    public DayOfWeek(Location location, Expression field, TimeZone timeZone) {
        super(location, field, timeZone, NonIsoDateTimeExtractor.DAY_OF_WEEK);
    }

    @Override
    protected NodeCtor2<Expression, TimeZone, BaseDateTimeFunction> ctorForInfo() {
        return DayOfWeek::new;
    }

    @Override
    protected DayOfWeek replaceChild(Expression newChild) {
        return new DayOfWeek(location(), newChild, timeZone());
    }
}