/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Result;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * One time query builder for records. Sets default values for the following
 * parameters:
 * <ul>
 * <li>From- Skip the first N records. This parameter is for paging if not
 * required set to 0. Default = 0</li>
 * <li>Size- Take only this number of records. Default =
 * {@value DEFAULT_SIZE}</li>
 * <li>IncludeInterim- Include interim results. Default = false</li>
 * <li>SortField- The field to sort results by if <code>null</code> no sort is
 * applied. Default = null</li>
 * <li>SortDescending- Sort in descending order. Default = true</li>
 * <li>recordScoreThreshold- Return only records with a record_score &gt;=
 * this value. Default = 0.0</li>
 * <li>start- The start bucket time. A bucket with this timestamp will be
 * included in the results. If 0 all buckets up to <code>endEpochMs</code> are
 * returned. Default = -1</li>
 * <li>end- The end bucket timestamp buckets up to but NOT including this
 * timestamp are returned. If 0 all buckets from <code>startEpochMs</code> are
 * returned. Default = -1</li>
 * </ul>
 */
public final class RecordsQueryBuilder {

    public static final int DEFAULT_SIZE = 100;

    private static final List<String> SECONDARY_SORT = Arrays.asList(
        AnomalyRecord.RECORD_SCORE.getPreferredName(),
        AnomalyRecord.OVER_FIELD_VALUE.getPreferredName(),
        AnomalyRecord.PARTITION_FIELD_VALUE.getPreferredName(),
        AnomalyRecord.BY_FIELD_VALUE.getPreferredName(),
        AnomalyRecord.FIELD_NAME.getPreferredName(),
        AnomalyRecord.FUNCTION.getPreferredName()
    );

    private int from = 0;
    private int size = DEFAULT_SIZE;
    private boolean includeInterim = false;
    private String sortField;
    private boolean sortDescending = true;
    private double recordScore = 0.0;
    private String start;
    private String end;
    private Date timestamp;

    public RecordsQueryBuilder from(int from) {
        this.from = from;
        return this;
    }

    public RecordsQueryBuilder size(int size) {
        this.size = size;
        return this;
    }

    public RecordsQueryBuilder epochStart(String startTime) {
        this.start = startTime;
        return this;
    }

    public RecordsQueryBuilder epochEnd(String endTime) {
        this.end = endTime;
        return this;
    }

    public RecordsQueryBuilder includeInterim(boolean include) {
        this.includeInterim = include;
        return this;
    }

    public RecordsQueryBuilder sortField(String fieldname) {
        this.sortField = fieldname;
        return this;
    }

    public RecordsQueryBuilder sortDescending(boolean sortDescending) {
        this.sortDescending = sortDescending;
        return this;
    }

    public RecordsQueryBuilder recordScore(double recordScore) {
        this.recordScore = recordScore;
        return this;
    }

    public RecordsQueryBuilder timestamp(Date timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public SearchSourceBuilder build() {
        QueryBuilder query = new ResultsFilterBuilder().timeRange(Result.TIMESTAMP.getPreferredName(), start, end)
            .score(AnomalyRecord.RECORD_SCORE.getPreferredName(), recordScore)
            .interim(includeInterim)
            .build();

        FieldSortBuilder sb;
        if (sortField != null) {
            sb = new FieldSortBuilder(sortField).missing("_last").order(sortDescending ? SortOrder.DESC : SortOrder.ASC);
        } else {
            sb = SortBuilders.fieldSort(ElasticsearchMappings.ES_DOC);
        }

        BoolQueryBuilder recordFilter = new BoolQueryBuilder().filter(query)
            .filter(new TermsQueryBuilder(Result.RESULT_TYPE.getPreferredName(), AnomalyRecord.RESULT_TYPE_VALUE));
        if (timestamp != null) {
            recordFilter.filter(QueryBuilders.termQuery(Result.TIMESTAMP.getPreferredName(), timestamp.getTime()));
        }

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().from(from)
            .size(size)
            .query(recordFilter)
            .sort(sb)
            .fetchSource(true);

        for (String eachSortField : SECONDARY_SORT) {
            searchSourceBuilder.sort(eachSortField, sortDescending ? SortOrder.DESC : SortOrder.ASC);
        }

        return searchSourceBuilder;
    }
}
