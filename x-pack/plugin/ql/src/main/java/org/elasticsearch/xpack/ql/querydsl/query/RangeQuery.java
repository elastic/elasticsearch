/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.querydsl.query;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;
import java.util.Objects;

import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

public class RangeQuery extends LeafQuery {

    private final String field;
    private final Object lower, upper;
    private final boolean includeLower, includeUpper;
    private final String format;
    private final ZoneId zoneId;

    public RangeQuery(Source source, String field, Object lower, boolean includeLower, Object upper, boolean includeUpper, ZoneId zoneId) {
        this(source, field, lower, includeLower, upper, includeUpper, null, zoneId);
    }

    public RangeQuery(Source source, String field, Object lower, boolean includeLower, Object upper,
            boolean includeUpper, String format, ZoneId zoneId) {
        super(source);
        this.field = field;
        this.lower = lower;
        this.upper = upper;
        this.includeLower = includeLower;
        this.includeUpper = includeUpper;
        this.format = format;
        this.zoneId = zoneId;
    }

    public String field() {
        return field;
    }

    public Object lower() {
        return lower;
    }

    public Object upper() {
        return upper;
    }

    public boolean includeLower() {
        return includeLower;
    }

    public boolean includeUpper() {
        return includeUpper;
    }

    public String format() {
        return format;
    }

    public ZoneId zoneId() {
        return zoneId;
    }

    @Override
    public QueryBuilder asBuilder() {
        RangeQueryBuilder queryBuilder = rangeQuery(field).from(lower, includeLower).to(upper, includeUpper);
        if (Strings.hasText(format)) {
            queryBuilder.format(format);
        }
        if (zoneId != null) {
            queryBuilder.timeZone(zoneId.getId());
        }

        return queryBuilder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, lower, upper, includeLower, includeUpper, format, zoneId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        RangeQuery other = (RangeQuery) obj;
        return Objects.equals(field, other.field) &&
                Objects.equals(includeLower, other.includeLower) &&
                Objects.equals(includeUpper, other.includeUpper) &&
                Objects.equals(lower, other.lower) &&
                Objects.equals(upper, other.upper) &&
                Objects.equals(format, other.format) &&
                Objects.equals(zoneId, other.zoneId);
    }

    @Override
    protected String innerToString() {
        return field + ":"
            + (includeLower ? "[" : "(") + lower + ", "
            + upper + (includeUpper ? "]" : ")") + "@" + zoneId.getId();
    }
}
