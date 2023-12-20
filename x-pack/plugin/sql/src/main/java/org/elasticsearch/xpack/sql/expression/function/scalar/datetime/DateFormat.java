/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;

import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeFormatProcessor.Formatter.DATE_FORMAT;

public class DateFormat extends BaseDateTimeFormatFunction {

    public DateFormat(Source source, Expression timestamp, Expression pattern, ZoneId zoneId) {
        super(source, timestamp, pattern, zoneId);
    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression newLeft, Expression newRight) {
        return new DateFormat(source(), newLeft, newRight, zoneId());
    }

    @Override
    protected DateTimeFormatProcessor.Formatter formatter() {
        return DATE_FORMAT;
    }

    @Override
    protected String scriptMethodName() {
        return "dateFormat";
    }

    @Override
    protected NodeInfo.NodeCtor3<Expression, Expression, ZoneId, BaseDateTimeFormatFunction> ctor() {
        return DateFormat::new;
    }
}
