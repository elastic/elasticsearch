/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.ml.utils.QueryBuilderHelper;

/**
 * Query builder for {@link ScheduledEvent}s
 * If <code>calendarIds</code> are not set then all calendars will match.
 */
public class ScheduledEventsQueryBuilder {
    public static final int DEFAULT_SIZE = 1000;

    private Integer from = 0;
    private Integer size = DEFAULT_SIZE;

    private String[] calendarIds;
    private String start;
    private String end;

    public static ScheduledEventsQueryBuilder builder() {
        return new ScheduledEventsQueryBuilder();
    }

    public ScheduledEventsQueryBuilder calendarIds(String[] calendarIds) {
        this.calendarIds = calendarIds;
        return this;
    }

    public ScheduledEventsQueryBuilder start(String start) {
        this.start = start;
        return this;
    }

    public ScheduledEventsQueryBuilder start(long start) {
        this.start = Long.toString(start);
        return this;
    }

    public ScheduledEventsQueryBuilder end(String end) {
        this.end = end;
        return this;
    }

    /**
     * Set the query from parameter.
     * @param from If null then no from will be set
     * @return this
     */
    public ScheduledEventsQueryBuilder from(Integer from) {
        this.from = from;
        return this;
    }

    /**
     * Set the query size parameter.
     * @param size If null then no size will be set
     * @return this
     */
    public ScheduledEventsQueryBuilder size(Integer size) {
        this.size = size;
        return this;
    }

    public SearchSourceBuilder build() {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery(ScheduledEvent.TYPE.getPreferredName(), ScheduledEvent.SCHEDULED_EVENT_TYPE));
        if (start != null) {
            RangeQueryBuilder startQuery = QueryBuilders.rangeQuery(ScheduledEvent.END_TIME.getPreferredName());
            startQuery.gt(start);
            boolQueryBuilder.filter(startQuery);
        }
        if (end != null) {
            RangeQueryBuilder endQuery = QueryBuilders.rangeQuery(ScheduledEvent.START_TIME.getPreferredName());
            endQuery.lt(end);
            boolQueryBuilder.filter(endQuery);
        }

        QueryBuilderHelper.buildTokenFilterQuery(Calendar.ID.getPreferredName(), calendarIds).ifPresent(boolQueryBuilder::filter);

        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
            .sort(ScheduledEvent.START_TIME.getPreferredName())
            .sort(ScheduledEvent.DESCRIPTION.getPreferredName())
            .query(boolQueryBuilder);
        if (from != null) {
            searchSourceBuilder.from(from);
        }
        if (size != null) {
            searchSourceBuilder.size(size);
        }
        return searchSourceBuilder;
    }
}
