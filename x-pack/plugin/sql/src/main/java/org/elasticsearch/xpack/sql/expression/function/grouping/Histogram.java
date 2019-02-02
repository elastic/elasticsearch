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
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class Histogram extends GroupingFunction {

    private final Literal interval;
    private final ZoneId zoneId;

    public Histogram(Location location, Expression field, Expression interval, ZoneId zoneId) {
        super(location, field, Collections.singletonList(interval));
        this.interval = Literal.of(interval);
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
    public final GroupingFunction replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 2) {
            throw new IllegalArgumentException("expected [2] children but received [" + newChildren.size() + "]");
        }
        return new Histogram(location(), newChildren.get(0), newChildren.get(1), zoneId);
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