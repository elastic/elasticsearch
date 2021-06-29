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
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.ZoneId;

import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeParseProcessor.Parser.DATE_TIME;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeParseProcessor.Parser;

public class DateTimeParse extends BaseDateTimeParseFunction {

    public DateTimeParse(Source source, Expression timestamp, Expression pattern, ZoneId zoneId) {
        super(source, timestamp, pattern, zoneId);
    }

    @Override
    protected Parser parser() {
        return DATE_TIME;
    }

    @Override
    protected NodeInfo.NodeCtor3<Expression, Expression, ZoneId, BaseDateTimeParseFunction> ctorForInfo() {
        return DateTimeParse::new;
    }

    @Override
    public DataType dataType() {
        return DataTypes.DATETIME;
    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression timestamp, Expression pattern) {
        return new DateTimeParse(source(), timestamp, pattern, zoneId());
    }

    @Override
    protected String scriptMethodName() {
        return "dateTimeParse";
    }

}
