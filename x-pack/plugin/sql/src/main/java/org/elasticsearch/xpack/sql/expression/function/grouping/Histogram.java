/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.grouping;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

import java.util.List;
import java.util.Objects;
import java.util.TimeZone;

public class Histogram extends GroupingFunction {

    private final Literal interval;
    private final TimeZone timeZone;

    public Histogram(Location location, Expression field, Expression interval, TimeZone timeZone) {
        super(location, field);
        this.interval = (Literal) interval;
        this.timeZone = timeZone;
    }

    public Literal interval() {
        return interval;
    }

    public TimeZone timeZone() {
        return timeZone;
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = Expressions.typeMustBeNumericOrDate(field(), "HISTOGRAM", ParamOrdinal.FIRST);
        if (resolution == TypeResolution.TYPE_RESOLVED) {
            // interval must be Literal interval
            if (field().dataType() == DataType.DATE) {
                resolution = Expressions.typeMustBe(interval, DataTypes::isInterval, "(Date) HISTOGRAM", ParamOrdinal.SECOND, "interval");
            } else {
                resolution = Expressions.typeMustBeNumeric(interval, "(Numeric) HISTOGRAM", ParamOrdinal.SECOND);
            }
        }

        return resolution;
    }


    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Histogram(location(), newChildren.get(0), interval, timeZone);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Histogram::new, field(), interval, timeZone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field(), interval, timeZone);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            Histogram other = (Histogram) obj;
            return Objects.equals(interval, other.interval)
                    && Objects.equals(timeZone, other.timeZone);
        }
        return false;
    }
}