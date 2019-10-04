/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.querydsl.container.Sort.Direction;

import java.time.ZoneId;
import java.util.Objects;

/**
 * GROUP BY key based on histograms on date/datetime fields.
 */
public class GroupByDateHistogram extends GroupByKey {

    private final long interval;
    private final ZoneId zoneId;

    public GroupByDateHistogram(String id, String fieldName, long interval, ZoneId zoneId) {
        this(id, fieldName, null, null, interval, zoneId);
    }

    public GroupByDateHistogram(String id, ScriptTemplate script, long interval, ZoneId zoneId) {
        this(id, null, script, null, interval, zoneId);
    }

    private GroupByDateHistogram(String id, String fieldName, ScriptTemplate script, Direction direction, long interval,
            ZoneId zoneId) {
        super(id, fieldName, script, direction);
        this.interval = interval;
        this.zoneId = zoneId;

    }

    // For testing
    public long interval() {
        return interval;
    }

    @Override
    protected CompositeValuesSourceBuilder<?> createSourceBuilder() {
        return new DateHistogramValuesSourceBuilder(id())
                .fixedInterval(new DateHistogramInterval(interval + "ms"))
                .timeZone(zoneId);
    }

    @Override
    protected GroupByKey copy(String id, String fieldName, ScriptTemplate script, Direction direction) {
        return new GroupByDateHistogram(id, fieldName, script, direction, interval, zoneId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), interval, zoneId);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            GroupByDateHistogram other = (GroupByDateHistogram) obj;
            return Objects.equals(interval, other.interval)
                    && Objects.equals(zoneId, other.zoneId);
        }
        return false;
    }
}
