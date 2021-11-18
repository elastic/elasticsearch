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
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.Result;

/**
 * One time query builder for buckets.
 * <ul>
 * <li>From- Skip the first N Buckets. This parameter is for paging if not
 * required set to 0. Default = 0</li>
 * <li>Size- Take only this number of Buckets. Default =
 * {@value DEFAULT_SIZE}</li>
 * <li>Expand- Include anomaly records. Default= false</li>
 * <li>IncludeInterim- Include interim results. Default = false</li>
 * <li>anomalyScoreThreshold- Return only buckets with an anomalyScore &gt;=
 * this value. Default = 0.0</li>
 * <li>start- The start bucket time. A bucket with this timestamp will be
 * included in the results. If 0 all buckets up to <code>endEpochMs</code> are
 * returned. Default = -1</li>
 * <li>end- The end bucket timestamp buckets up to but NOT including this
 * timestamp are returned. If 0 all buckets from <code>startEpochMs</code> are
 * returned. Default = -1</li>
 * <li>partitionValue Set the bucket's max normalized probability to this
 * partition field value's max normalized probability. Default = null</li>
 * </ul>
 */
public final class BucketsQueryBuilder {
    public static final int DEFAULT_SIZE = 100;

    private int from = 0;
    private int size = DEFAULT_SIZE;
    private boolean expand = false;
    private boolean includeInterim = false;
    private double anomalyScoreFilter = 0.0;
    private String start;
    private String end;
    private String timestamp;
    private String sortField = Result.TIMESTAMP.getPreferredName();
    private boolean sortDescending = false;

    public BucketsQueryBuilder from(int from) {
        this.from = from;
        return this;
    }

    public BucketsQueryBuilder size(int size) {
        this.size = size;
        return this;
    }

    public BucketsQueryBuilder expand(boolean expand) {
        this.expand = expand;
        return this;
    }

    public boolean isExpand() {
        return expand;
    }

    public BucketsQueryBuilder includeInterim(boolean include) {
        this.includeInterim = include;
        return this;
    }

    public boolean isIncludeInterim() {
        return includeInterim;
    }

    public BucketsQueryBuilder anomalyScoreThreshold(Double anomalyScoreFilter) {
        if (anomalyScoreFilter != null) {
            this.anomalyScoreFilter = anomalyScoreFilter;
        }
        return this;
    }

    public BucketsQueryBuilder sortField(String sortField) {
        this.sortField = sortField;
        return this;
    }

    public BucketsQueryBuilder sortDescending(boolean sortDescending) {
        this.sortDescending = sortDescending;
        return this;
    }

    /**
     * If startTime &lt;= 0 the parameter is not set
     */
    public BucketsQueryBuilder start(String startTime) {
        this.start = startTime;
        return this;
    }

    /**
     * If endTime &lt;= 0 the parameter is not set
     */
    public BucketsQueryBuilder end(String endTime) {
        this.end = endTime;
        return this;
    }

    public BucketsQueryBuilder timestamp(String timestamp) {
        this.timestamp = timestamp;
        this.size = 1;
        return this;
    }

    public boolean hasTimestamp() {
        return timestamp != null;
    }

    public SearchSourceBuilder build() {
        if (timestamp != null && (start != null || end != null)) {
            throw new IllegalStateException("Either specify timestamp or start/end");
        }

        ResultsFilterBuilder rfb = new ResultsFilterBuilder();
        if (hasTimestamp()) {
            rfb.timeRange(Result.TIMESTAMP.getPreferredName(), timestamp);
        } else {
            rfb.timeRange(Result.TIMESTAMP.getPreferredName(), start, end)
                .score(Bucket.ANOMALY_SCORE.getPreferredName(), anomalyScoreFilter)
                .interim(includeInterim);
        }

        SortBuilder<?> sortBuilder = new FieldSortBuilder(sortField).order(sortDescending ? SortOrder.DESC : SortOrder.ASC);

        QueryBuilder boolQuery = new BoolQueryBuilder().filter(rfb.build())
            .filter(QueryBuilders.termQuery(Result.RESULT_TYPE.getPreferredName(), Bucket.RESULT_TYPE_VALUE));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.sort(sortBuilder);
        searchSourceBuilder.query(boolQuery);
        searchSourceBuilder.from(from);
        searchSourceBuilder.size(size);

        // If not using the default sort field (timestamp) add it as a secondary sort
        if (Result.TIMESTAMP.getPreferredName().equals(sortField) == false) {
            searchSourceBuilder.sort(Result.TIMESTAMP.getPreferredName(), sortDescending ? SortOrder.DESC : SortOrder.ASC);
        }

        return searchSourceBuilder;
    }
}
