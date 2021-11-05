/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo.NodeCtor2;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.NonIsoDateTimeProcessor.NonIsoDateTimeExtractor;

import java.time.ZoneId;

/**
 * Extract the week of the year from a datetime following the non-ISO standard.
 */
public class WeekOfYear extends NonIsoDateTimeFunction {

    public WeekOfYear(Source source, Expression field, ZoneId zoneId) {
        super(source, field, zoneId, NonIsoDateTimeExtractor.WEEK_OF_YEAR);
    }

    @Override
    protected NodeCtor2<Expression, ZoneId, BaseDateTimeFunction> ctorForInfo() {
        return WeekOfYear::new;
    }

    @Override
    protected WeekOfYear replaceChild(Expression newChild) {
        return new WeekOfYear(source(), newChild, zoneId());
    }
}
