/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeFormatProcessor.Formatter;

import java.time.ZoneId;

public class DateFormat extends BaseDateTimeFormatFunction {
    public DateFormat(Source source, Expression timestamp, Expression pattern, ZoneId zoneId) {
        super(source, timestamp, pattern, zoneId);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DateFormat::new, left(), right(), zoneId());
    }

    @Override
    protected Formatter formatter() {
        return Formatter.DATE_FORMAT;
    }

    @Override
    protected NodeInfo.NodeCtor3<Expression, Expression, ZoneId, BaseDateTimeFormatFunction> ctor() {
        return DateFormat::new;
    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression timestamp, Expression pattern) {
        return new DateFormat(source(), timestamp, pattern, zoneId());
    }
}
