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
 * Extract the month from a datetime in text format (January, February etc.)
 */
public class MonthName extends NamedDateTimeFunction {
    
    public MonthName(Location location, Expression field, TimeZone timeZone) {
        super(location, field, timeZone, NameExtractor.MONTH_NAME);
    }

    @Override
    protected NodeCtor2<Expression, TimeZone, BaseDateTimeFunction> ctorForInfo() {
        return MonthName::new;
    }

    @Override
    protected MonthName replaceChild(Expression newChild) {
        return new MonthName(location(), newChild, timeZone());
    }
}