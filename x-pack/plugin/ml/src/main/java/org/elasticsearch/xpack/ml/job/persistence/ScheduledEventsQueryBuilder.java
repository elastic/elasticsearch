/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * Query builder for {@link ScheduledEvent}s
 * If <code>calendarIds</code> are not set then all calendars will match.
 */
public class ScheduledEventsQueryBuilder {
    public static final int DEFAULT_SIZE = 1000;

    private Integer from = 0;
    private Integer size = DEFAULT_SIZE;

    private List<String> calendarIds;
    private String start;
    private String end;

    public ScheduledEventsQueryBuilder calendarIds(List<String> calendarIds) {
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
        List<QueryBuilder> queries = new ArrayList<>();

        if (start != null) {
            RangeQueryBuilder startQuery = QueryBuilders.rangeQuery(ScheduledEvent.END_TIME.getPreferredName());
            startQuery.gt(start);
            queries.add(startQuery);
        }
        if (end != null) {
            RangeQueryBuilder endQuery = QueryBuilders.rangeQuery(ScheduledEvent.START_TIME.getPreferredName());
            endQuery.lt(end);
            queries.add(endQuery);
        }

        if (calendarIds != null && calendarIds.isEmpty() == false) {
            queries.add(new TermsQueryBuilder(Calendar.ID.getPreferredName(), calendarIds));
        }

        QueryBuilder typeQuery = new TermsQueryBuilder(ScheduledEvent.TYPE.getPreferredName(), ScheduledEvent.SCHEDULED_EVENT_TYPE);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.sort(ScheduledEvent.START_TIME.getPreferredName());
        searchSourceBuilder.sort(ScheduledEvent.DESCRIPTION.getPreferredName());
        if (from != null) {
            searchSourceBuilder.from(from);
        }
        if (size != null) {
            searchSourceBuilder.size(size);
        }

        if (queries.isEmpty()) {
            searchSourceBuilder.query(typeQuery);
        } else  {
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            boolQueryBuilder.filter(typeQuery);
            for (QueryBuilder query : queries) {
                boolQueryBuilder.filter(query);
            }
            searchSourceBuilder.query(boolQueryBuilder);
        }

        return searchSourceBuilder;
    }
}
