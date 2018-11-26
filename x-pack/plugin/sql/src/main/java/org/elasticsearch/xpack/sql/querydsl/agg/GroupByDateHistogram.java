/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.querydsl.container.Sort.Direction;
import org.joda.time.DateTimeZone;

import java.util.Objects;
import java.util.TimeZone;

/**
 * GROUP BY key based on histograms on date fields.
 */
public class GroupByDateHistogram extends GroupByKey {

    private final long interval;
    private final TimeZone timeZone;

    public GroupByDateHistogram(String id, String fieldName, long interval, TimeZone timeZone) {
        this(id, fieldName, null, null, interval, timeZone);
    }

    public GroupByDateHistogram(String id, ScriptTemplate script, long interval, TimeZone timeZone) {
        this(id, null, script, null, interval, timeZone);
    }

    private GroupByDateHistogram(String id, String fieldName, ScriptTemplate script, Direction direction, long interval,
            TimeZone timeZone) {
        super(id, fieldName, script, direction);
        this.interval = interval;
        this.timeZone = timeZone;

    }

    @Override
    protected CompositeValuesSourceBuilder<?> createSourceBuilder() {
        return new DateHistogramValuesSourceBuilder(id())
                .interval(interval)
                .timeZone(DateTimeZone.forTimeZone(timeZone));
    }

    @Override
    protected GroupByKey copy(String id, String fieldName, ScriptTemplate script, Direction direction) {
        return new GroupByDateHistogram(id, fieldName, script, direction, interval, timeZone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), interval, timeZone);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            GroupByDateHistogram other = (GroupByDateHistogram) obj;
            return Objects.equals(interval, other.interval)
                    && Objects.equals(timeZone, other.timeZone);
        }
        return false;
    }
}
