/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CalendarQueryBuilder {

    private PageParams pageParams = new PageParams(0, 10000);
    private String jobId;
    private List<String> jobGroups = Collections.emptyList();
    private boolean sort = false;

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
        QueryBuilder qb;
        List<String> jobIdAndGroups = new ArrayList<>(jobGroups);
        if (jobId != null) {
            jobIdAndGroups.add(jobId);
        }

        if (jobIdAndGroups.isEmpty() == false) {
            qb = new BoolQueryBuilder()
                    .filter(new TermsQueryBuilder(Calendar.TYPE.getPreferredName(), Calendar.CALENDAR_TYPE))
                    .filter(new TermsQueryBuilder(Calendar.JOB_IDS.getPreferredName(), jobIdAndGroups));
        } else {
            qb = new TermsQueryBuilder(Calendar.TYPE.getPreferredName(), Calendar.CALENDAR_TYPE);
        }

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(qb);

        if (sort) {
            sourceBuilder.sort(Calendar.ID.getPreferredName());
        }

        sourceBuilder.from(pageParams.getFrom()).size(pageParams.getSize());

        return sourceBuilder;
    }
}
