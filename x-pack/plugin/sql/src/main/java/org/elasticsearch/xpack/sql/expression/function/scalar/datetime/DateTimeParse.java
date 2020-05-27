/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.ZoneId;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;

public class DateTimeParse extends BinaryDateTimeFunction {

    public DateTimeParse(Source source, Expression timestamp, Expression pattern, ZoneId zoneId) {
        super(source, timestamp, pattern, zoneId);
    }

    @Override
    public DataType dataType() {
        return DataTypes.DATETIME;
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = isString(left(), sourceText(), Expressions.ParamOrdinal.FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }
        resolution = isString(right(), sourceText(), Expressions.ParamOrdinal.SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    protected BinaryScalarFunction replaceChildren(Expression timestamp, Expression pattern) {
        return new DateTimeParse(source(), timestamp, pattern, zoneId());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DateTimeParse::new, left(), right(), zoneId());
    }

    @Override
    protected String scriptMethodName() {
        return "dateTimeParse";
    }

    @Override
    public Object fold() {
        return DateTimeParseProcessor.process(left().fold(), right().fold(), zoneId());
    }

    @Override
    protected Pipe createPipe(Pipe timestamp, Pipe pattern, ZoneId zoneId) {
        return new DateTimeParsePipe(source(), this, timestamp, pattern, zoneId);
    }
}
