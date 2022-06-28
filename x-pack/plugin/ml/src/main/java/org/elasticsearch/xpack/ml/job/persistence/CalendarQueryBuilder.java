/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.ml.utils.QueryBuilderHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CalendarQueryBuilder {

    private PageParams pageParams = new PageParams(0, 10000);
    private String jobId;
    private List<String> jobGroups = Collections.emptyList();
    private boolean sort = false;
    private String[] idTokens = new String[0];

    public static CalendarQueryBuilder builder() {
        return new CalendarQueryBuilder();
    }

    /**
     * Page the query result
     * @param params The page parameters
     * @return this
     */
    public CalendarQueryBuilder pageParams(PageParams params) {
        this.pageParams = params;
        return this;
    }

    /**
     * Query only calendars used by this job
     * @param jobId The job Id
     * @return this
     */
    public CalendarQueryBuilder jobId(String jobId) {
        this.jobId = jobId;
        return this;
    }

    public CalendarQueryBuilder jobGroups(List<String> jobGroups) {
        this.jobGroups = jobGroups;
        return this;
    }

    public CalendarQueryBuilder calendarIdTokens(String[] idTokens) {
        this.idTokens = idTokens;
        return this;
    }

    public boolean isForAllCalendars() {
        return Strings.isAllOrWildcard(idTokens);
    }

    public Exception buildNotFoundException() {
        return new ResourceNotFoundException("No calendar with id [" + Strings.arrayToCommaDelimitedString(idTokens) + "]");
    }

    /**
     * Sort results by calendar_id
     * @param sort Sort if true
     * @return this
     */
    public CalendarQueryBuilder sort(boolean sort) {
        this.sort = sort;
        return this;
    }

    public SearchSourceBuilder build() {
        BoolQueryBuilder qb = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery(Calendar.TYPE.getPreferredName(), Calendar.CALENDAR_TYPE));
        List<String> jobIdAndGroups = new ArrayList<>(jobGroups);
        if (jobId != null) {
            jobIdAndGroups.add(jobId);
        }

        if (jobIdAndGroups.isEmpty() == false) {
            jobIdAndGroups.add(Metadata.ALL);
            qb.filter(new TermsQueryBuilder(Calendar.JOB_IDS.getPreferredName(), jobIdAndGroups));
        }
        QueryBuilderHelper.buildTokenFilterQuery(Calendar.ID.getPreferredName(), idTokens).ifPresent(qb::filter);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(qb);

        if (sort) {
            sourceBuilder.sort(Calendar.ID.getPreferredName());
        }

        sourceBuilder.from(pageParams.getFrom()).size(pageParams.getSize());

        return sourceBuilder;
    }
}
