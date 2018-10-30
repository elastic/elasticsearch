/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xpack.sql.querydsl.container.Sort.Direction;
import org.joda.time.DateTimeZone;

import java.util.Objects;
import java.util.TimeZone;

/**
 * GROUP BY key specific for date fields.
 */
public class GroupByDateKey extends GroupByKey {

    private final String interval;
    private final TimeZone timeZone;

    public GroupByDateKey(String id, String fieldName, String interval, TimeZone timeZone) {
        this(id, fieldName, null, interval, timeZone);
    }

    public GroupByDateKey(String id, String fieldName, Direction direction, String interval, TimeZone timeZone) {
        super(id, fieldName, direction);
        this.interval = interval;
        this.timeZone = timeZone;
    }

    public String interval() {
        return interval;
    }

    public TimeZone timeZone() {
        return timeZone;
    }

    @Override
    public DateHistogramValuesSourceBuilder asValueSource() {
        return new DateHistogramValuesSourceBuilder(id())
                .field(fieldName())
                .dateHistogramInterval(new DateHistogramInterval(interval))
                .timeZone(DateTimeZone.forTimeZone(timeZone))
                .missingBucket(true);
    }

    @Override
    protected GroupByKey copy(String id, String fieldName, Direction direction) {
        return new GroupByDateKey(id, fieldName, direction, interval, timeZone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), interval, timeZone);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            GroupByDateKey other = (GroupByDateKey) obj;
            return Objects.equals(interval, other.interval)
                    && Objects.equals(timeZone, other.timeZone);
        }
        return false;
    }
}
