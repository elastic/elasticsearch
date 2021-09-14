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
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeParseProcessor.Parser;

public abstract class BaseDateTimeParseFunction extends BinaryDateTimeFunction {

    public BaseDateTimeParseFunction(Source source, Expression datePart, Expression timestamp, ZoneId zoneId) {
        super(source, datePart, timestamp, zoneId);
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = isString(left(), sourceText(), FIRST);
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
    public Object fold() {
        return parser().parse(left().fold(), right().fold(), zoneId());
    }

    @Override
    protected Pipe createPipe(Pipe timestamp, Pipe pattern, ZoneId zoneId) {
        return new DateTimeParsePipe(source(), this, timestamp, pattern, zoneId, parser());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ctorForInfo(), left(), right(), zoneId());
    }

    protected abstract Parser parser();

    protected abstract NodeInfo.NodeCtor3<Expression, Expression, ZoneId, BaseDateTimeParseFunction> ctorForInfo();
}
