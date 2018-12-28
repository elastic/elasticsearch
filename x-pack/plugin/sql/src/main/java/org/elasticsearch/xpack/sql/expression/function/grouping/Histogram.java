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
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

import java.time.ZoneId;
import java.util.Objects;

public class Histogram extends GroupingFunction {

    private final Literal interval;
    private final ZoneId zoneId;

    public Histogram(Source source, Expression field, Expression interval, ZoneId zoneId) {
        super(source, field);
        this.interval = (Literal) interval;
        this.zoneId = zoneId;
    }

    public Literal interval() {
        return interval;
    }

    public ZoneId zoneId() {
        return zoneId;
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
    protected GroupingFunction replaceChild(Expression newChild) {
        return new Histogram(source(), newChild, interval, zoneId);
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Histogram::new, field(), interval, zoneId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field(), interval, zoneId);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            Histogram other = (Histogram) obj;
            return Objects.equals(interval, other.interval)
                    && Objects.equals(zoneId, other.zoneId);
        }
        return false;
    }
}