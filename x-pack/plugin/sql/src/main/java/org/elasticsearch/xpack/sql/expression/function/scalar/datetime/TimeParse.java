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
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;

import java.time.ZoneId;

import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeParseProcessor.Parser.TIME;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeParseProcessor.Parser;

public class TimeParse extends BaseDateTimeParseFunction {
    
    public TimeParse(Source source, Expression timestamp, Expression pattern, ZoneId zoneId) {
        super(source, timestamp, pattern, zoneId);
    }

    @Override
    protected Parser parser() {
        return TIME;
    }
    
    @Override
    protected NodeInfo.NodeCtor3<Expression, Expression, ZoneId, BaseDateTimeParseFunction> ctorForInfo() {
        return TimeParse::new;
    }

    @Override
    public DataType dataType() {
        return SqlDataTypes.TIME;
    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression timestamp, Expression pattern) {
        return new TimeParse(source(), timestamp, pattern, zoneId());
    }

    @Override
    protected String scriptMethodName() {
        return "timeParse";
    }
    
}
