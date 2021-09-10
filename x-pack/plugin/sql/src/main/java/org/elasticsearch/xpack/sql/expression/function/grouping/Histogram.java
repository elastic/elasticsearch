/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.grouping;

import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.sql.expression.SqlTypeResolutions.isNumericOrDate;

public class Histogram extends GroupingFunction {

    private final Literal interval;
    private final ZoneId zoneId;
    public static String YEAR_INTERVAL = DateHistogramInterval.YEAR.toString();
    public static String MONTH_INTERVAL = DateHistogramInterval.MONTH.toString();
    public static String DAY_INTERVAL = DateHistogramInterval.DAY.toString();

    public Histogram(Source source, Expression field, Expression interval, ZoneId zoneId) {
        super(source, field, Collections.singletonList(interval));
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
        TypeResolution resolution = isNumericOrDate(field(), "HISTOGRAM", FIRST);
        if (resolution == TypeResolution.TYPE_RESOLVED) {
            // interval must be Literal interval
            if (SqlDataTypes.isDateBased(field().dataType())) {
                resolution = isType(
                    interval,
                    SqlDataTypes::isInterval,
                    "(Date) HISTOGRAM",
                    SECOND,
                    "interval"
                );
            } else {
                resolution = isNumeric(interval, "(Numeric) HISTOGRAM", SECOND);
            }
        }

        return resolution;
    }

    @Override
    public final GroupingFunction replaceChildren(List<Expression> newChildren) {
        return new Histogram(source(), newChildren.get(0), newChildren.get(1), zoneId);
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
