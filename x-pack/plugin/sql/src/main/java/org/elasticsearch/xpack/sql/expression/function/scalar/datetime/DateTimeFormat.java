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
import static org.elasticsearch.xpack.sql.expression.SqlTypeResolutions.isDateOrTime;

public class DateTimeFormat extends BinaryDateTimeFunction {

    public DateTimeFormat(Source source, Expression timestamp, Expression pattern, ZoneId zoneId) {
        super(source, timestamp, pattern, zoneId);
    }

    @Override
    public DataType dataType() {
        return DataTypes.KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = isDateOrTime(left(), sourceText(), Expressions.ParamOrdinal.FIRST);
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
        return new DateTimeFormat(source(), timestamp, pattern, zoneId());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DateTimeFormat::new, left(), right(), zoneId());
    }

    @Override
    protected String scriptMethodName() {
        return "dateTimeFormat";
    }

    @Override
    public Object fold() {
        return DateTimeFormatProcessor.process(left().fold(), right().fold(), zoneId());
    }

    @Override
    protected Pipe createPipe(Pipe timestamp, Pipe pattern, ZoneId zoneId) {
        return new DateTimeFormatPipe(source(), this, timestamp, pattern, zoneId);
    }
}
