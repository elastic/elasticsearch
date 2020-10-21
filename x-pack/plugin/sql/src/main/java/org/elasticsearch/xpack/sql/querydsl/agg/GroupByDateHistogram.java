/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.querydsl.container.Sort.Direction;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.time.ZoneId;
import java.util.Objects;

/**
 * GROUP BY key based on histograms on date/datetime fields.
 */
public class GroupByDateHistogram extends GroupByKey {

    private final long fixedInterval;
    private final String calendarInterval;
    private final ZoneId zoneId;

    public GroupByDateHistogram(String id, String fieldName, long fixedInterval, ZoneId zoneId) {
        this(id, AggSource.of(fieldName), null, fixedInterval, null, zoneId);
    }

    public GroupByDateHistogram(String id, ScriptTemplate script, long fixedInterval, ZoneId zoneId) {
        this(id, AggSource.of(script), null, fixedInterval, null, zoneId);
    }
    
    public GroupByDateHistogram(String id, String fieldName, String calendarInterval, ZoneId zoneId) {
        this(id, AggSource.of(fieldName), null, -1L, calendarInterval, zoneId);
    }
    
    public GroupByDateHistogram(String id, ScriptTemplate script, String calendarInterval, ZoneId zoneId) {
        this(id, AggSource.of(script), null, -1L, calendarInterval, zoneId);
    }

    private GroupByDateHistogram(String id, AggSource source, Direction direction, long fixedInterval,
                                 String calendarInterval, ZoneId zoneId) {
        super(id, source, direction);
        if (fixedInterval <= 0 && (calendarInterval == null || calendarInterval.isBlank())) {
            throw new SqlIllegalArgumentException("Either fixed interval or calendar interval needs to be specified");
        }
        this.fixedInterval = fixedInterval;
        this.calendarInterval = calendarInterval;
        this.zoneId = zoneId;
    }

    // For testing
    public long fixedInterval() {
        return fixedInterval;
    }

    @Override
    protected CompositeValuesSourceBuilder<?> createSourceBuilder() {
        DateHistogramValuesSourceBuilder builder = new DateHistogramValuesSourceBuilder(id()).timeZone(zoneId);
        return calendarInterval != null ? builder.calendarInterval(new DateHistogramInterval(calendarInterval))
                                        : builder.fixedInterval(new DateHistogramInterval(fixedInterval + "ms"));
    }

    @Override
    protected GroupByKey copy(String id, AggSource source, Direction direction) {
        return new GroupByDateHistogram(id, source(), direction, fixedInterval, calendarInterval, zoneId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fixedInterval, calendarInterval, zoneId);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            GroupByDateHistogram other = (GroupByDateHistogram) obj;
            return Objects.equals(fixedInterval, other.fixedInterval)
                    && Objects.equals(calendarInterval, other.calendarInterval)
                    && Objects.equals(zoneId, other.zoneId);
        }
        return false;
    }
}
