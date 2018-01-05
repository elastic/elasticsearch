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
import org.elasticsearch.xpack.ml.calendars.Calendar;
import org.elasticsearch.xpack.ml.calendars.ScheduledEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * Query builder for {@link ScheduledEvent}s
 * If <code>calendarIds</code> are not set then all calendars will match.
 */
public class ScheduledEventsQueryBuilder {
    public static final int DEFAULT_SIZE = 1000;

    private int from = 0;
    private int size = DEFAULT_SIZE;

    private List<String> calendarIds;
    private String after;
    private String before;

    public ScheduledEventsQueryBuilder calendarIds(List<String> calendarIds) {
        this.calendarIds = calendarIds;
        return this;
    }

    public ScheduledEventsQueryBuilder after(String after) {
        this.after = after;
        return this;
    }

    public ScheduledEventsQueryBuilder before(String before) {
        this.before = before;
        return this;
    }

    public ScheduledEventsQueryBuilder from(int from) {
        this.from = from;
        return this;
    }

    public ScheduledEventsQueryBuilder size(int size) {
        this.size = size;
        return this;
    }

    public SearchSourceBuilder build() {
        List<QueryBuilder> queries = new ArrayList<>();

        if (after != null) {
            RangeQueryBuilder afterQuery = QueryBuilders.rangeQuery(ScheduledEvent.END_TIME.getPreferredName());
            afterQuery.gt(after);
            queries.add(afterQuery);
        }
        if (before != null) {
            RangeQueryBuilder beforeQuery = QueryBuilders.rangeQuery(ScheduledEvent.START_TIME.getPreferredName());
            beforeQuery.lt(before);
            queries.add(beforeQuery);
        }

        if (calendarIds != null && calendarIds.isEmpty() == false) {
            queries.add(new TermsQueryBuilder(Calendar.ID.getPreferredName(), calendarIds));
        }

        QueryBuilder typeQuery = new TermsQueryBuilder(ScheduledEvent.TYPE.getPreferredName(), ScheduledEvent.SCHEDULED_EVENT_TYPE);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.sort(ScheduledEvent.START_TIME.getPreferredName());
        searchSourceBuilder.from(from);
        searchSourceBuilder.size(size);

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
