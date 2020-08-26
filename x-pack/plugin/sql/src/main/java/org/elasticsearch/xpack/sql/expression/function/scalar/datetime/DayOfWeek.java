/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo.NodeCtor2;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.NonIsoDateTimeProcessor.NonIsoDateTimeExtractor;

import java.time.ZoneId;

/**
 * Extract the day of the week from a datetime in non-ISO format. 1 is Sunday, 2 is Monday, etc.
 */
public class DayOfWeek extends NonIsoDateTimeFunction {
    
    public DayOfWeek(Source source, Expression field, ZoneId zoneId) {
        super(source, field, zoneId, NonIsoDateTimeExtractor.DAY_OF_WEEK);
    }

    @Override
    protected NodeCtor2<Expression, ZoneId, BaseDateTimeFunction> ctorForInfo() {
        return DayOfWeek::new;
    }

    @Override
    protected DayOfWeek replaceChild(Expression newChild) {
        return new DayOfWeek(source(), newChild, zoneId());
    }
}