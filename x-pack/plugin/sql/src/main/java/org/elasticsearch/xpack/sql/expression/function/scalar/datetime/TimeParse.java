/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;

import java.time.ZoneId;

import static org.elasticsearch.xpack.ql.type.DateUtils.UTC;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeParseProcessor.Parser.TIME;

public class TimeParse extends BaseDateTimeParseFunction {
    
    public TimeParse(Source source, Expression timestamp, Expression pattern) {
        super(source, timestamp, pattern, UTC);
    }

    @Override
    public DataType dataType() {
        return SqlDataTypes.TIME;
    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression timestamp, Expression pattern) {
        return new TimeParse(source(), timestamp, pattern);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, TimeParse::new, left(), right());
    }

    @Override
    protected String scriptMethodName() {
        return "timeParse";
    }

    @Override
    public Object fold() {
        return TIME.parse(left().fold(), right().fold(), zoneId());
    }

    @Override
    protected Pipe createPipe(Pipe timestamp, Pipe pattern, ZoneId zoneId) {
        return new DateTimeParsePipe(source(), this, timestamp, pattern, zoneId(), TIME);
    }
}
