/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.sql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;

import java.time.Instant;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;

import static org.elasticsearch.xpack.sql.expression.TypeResolutions.isDate;

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
        return isDate(field(), sourceText(), ParamOrdinal.DEFAULT);
    }

    public ZoneId zoneId() {
        return zoneId;
    }

    @Override
    public boolean foldable() {
        return field().foldable();
    }

    @Override
    public Object fold() {
        Object folded = field().fold();
        if (folded == null) {
            return null;
        }

        if (folded instanceof OffsetTime) {
            return doFold(((OffsetTime) folded).withOffsetSameInstant(zoneId.getRules().getOffset(Instant.now())));
        }
        if (folded instanceof ZonedDateTime) {
            return doFold(((ZonedDateTime) folded).withZoneSameInstant(zoneId));
        }

        throw new SqlIllegalArgumentException("A [date], a [time] or a [datetime] is required; received {}", field());
    }

    protected abstract Object doFold(ZonedDateTime dateTime);

    protected Object doFold(OffsetTime time) {
        throw new SqlIllegalArgumentException("Cannot operate on [time]");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        BaseDateTimeFunction other = (BaseDateTimeFunction) obj;
        return Objects.equals(other.field(), field())
                && Objects.equals(other.zoneId(), zoneId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field(), zoneId());
    }
}
