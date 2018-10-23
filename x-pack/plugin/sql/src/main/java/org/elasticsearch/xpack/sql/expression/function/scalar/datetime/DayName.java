/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.NamedDateTimeProcessor.NameExtractor;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo.NodeCtor2;

import java.util.TimeZone;

/**
 * Extract the day of the week from a datetime in text format (Monday, Tuesday etc.)
 */
public class DayName extends NamedDateTimeFunction {
    
    public DayName(Location location, Expression field, TimeZone timeZone) {
        super(location, field, timeZone, NameExtractor.DAY_NAME);
    }

    @Override
    protected NodeCtor2<Expression, TimeZone, BaseDateTimeFunction> ctorForInfo() {
        return DayName::new;
    }

    @Override
    protected DayName replaceChild(Expression newChild) {
        return new DayName(location(), newChild, timeZone());
    }
}