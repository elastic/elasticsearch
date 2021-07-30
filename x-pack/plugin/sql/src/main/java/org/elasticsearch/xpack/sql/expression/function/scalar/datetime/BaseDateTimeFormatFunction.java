/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.NodeInfo.NodeCtor3;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.ZoneId;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.sql.expression.SqlTypeResolutions.isDateOrTime;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeFormatProcessor.Formatter;

public abstract class BaseDateTimeFormatFunction extends BinaryDateTimeFunction {
    public BaseDateTimeFormatFunction(Source source, Expression timestamp, Expression pattern, ZoneId zoneId) {
        super(source, timestamp, pattern, zoneId);
    }

    @Override
    public DataType dataType() {
        return DataTypes.KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = isDateOrTime(left(), sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }
        resolution = isString(right(), sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ctor(), left(), right(), zoneId());
    }

    @Override
    public Object fold() {
        return formatter().format(left().fold(), right().fold(), zoneId());
    }

    @Override
    protected Pipe createPipe(Pipe timestamp, Pipe pattern, ZoneId zoneId) {
        return new DateTimeFormatPipe(source(), this, timestamp, pattern, zoneId, formatter());
    }

    protected abstract Formatter formatter();

    protected abstract NodeCtor3<Expression, Expression, ZoneId, BaseDateTimeFormatFunction> ctor();
}
