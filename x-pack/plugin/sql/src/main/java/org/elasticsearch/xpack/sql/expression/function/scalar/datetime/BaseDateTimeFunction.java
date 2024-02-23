/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;
import java.util.Objects;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.sql.expression.SqlTypeResolutions.isDate;

abstract class BaseDateTimeFunction extends UnaryScalarFunction {

    private final ZoneId zoneId;

    BaseDateTimeFunction(Source source, Expression field, ZoneId zoneId) {
        super(source, field);
        this.zoneId = zoneId;
    }

    @Override
    protected final NodeInfo<BaseDateTimeFunction> info() {
        return NodeInfo.create(this, ctorForInfo(), field(), zoneId());
    }

    protected abstract NodeInfo.NodeCtor2<Expression, ZoneId, BaseDateTimeFunction> ctorForInfo();

    @Override
    protected TypeResolution resolveType() {
        return isDate(field(), sourceText(), DEFAULT);
    }

    public ZoneId zoneId() {
        return zoneId;
    }

    @Override
    public boolean foldable() {
        return field().foldable();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), field(), zoneId());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        BaseDateTimeFunction other = (BaseDateTimeFunction) obj;
        return Objects.equals(other.field(), field()) && Objects.equals(other.zoneId(), zoneId());
    }
}
