/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.sql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.TimeZone;

abstract class BaseDateTimeFunction extends UnaryScalarFunction {
    
    private final TimeZone timeZone;
    private final ZoneId zoneId;
    private final String name;

    BaseDateTimeFunction(Location location, Expression field, TimeZone timeZone) {
        super(location, field);
        this.timeZone = timeZone;
        this.zoneId = timeZone != null ? timeZone.toZoneId() : null;

        StringBuilder sb = new StringBuilder(super.name());
        // add timezone as last argument
        sb.insert(sb.length() - 1, " [" + timeZone.getID() + "]");

        this.name = sb.toString();
    }

    @Override
    protected final NodeInfo<BaseDateTimeFunction> info() {
        return NodeInfo.create(this, ctorForInfo(), field(), timeZone());
    }

    protected abstract NodeInfo.NodeCtor2<Expression, TimeZone, BaseDateTimeFunction> ctorForInfo();

    @Override
    protected TypeResolution resolveType() {
        return Expressions.typeMustBeDate(field(), functionName(), ParamOrdinal.DEFAULT);
    }

    public TimeZone timeZone() {
        return timeZone;
    }
    
    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean foldable() {
        return field().foldable();
    }

    @Override
    public Object fold() {
        ZonedDateTime folded = (ZonedDateTime) field().fold();
        if (folded == null) {
            return null;
        }

        return doFold(folded.withZoneSameInstant(zoneId));
    }

    protected abstract Object doFold(ZonedDateTime dateTime);
    

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        BaseDateTimeFunction other = (BaseDateTimeFunction) obj;
        return Objects.equals(other.field(), field())
            && Objects.equals(other.timeZone(), timeZone());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field(), timeZone());
    }
}
