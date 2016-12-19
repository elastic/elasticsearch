/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;

/**
 * This builder facilitates the creation of a {@link QueryBuilder} with common
 * characteristics to both buckets and records.
 */
class ResultsFilterBuilder {
    private final List<QueryBuilder> filters;

    ResultsFilterBuilder() {
        filters = new ArrayList<>();
    }

    ResultsFilterBuilder(QueryBuilder filterBuilder) {
        this();
        filters.add(filterBuilder);
    }

    ResultsFilterBuilder timeRange(String field, Object start, Object end) {
        if (start != null || end != null) {
            RangeQueryBuilder timeRange = QueryBuilders.rangeQuery(field);
            if (start != null) {
                timeRange.gte(start);
            }
            if (end != null) {
                timeRange.lt(end);
            }
            addFilter(timeRange);
        }
        return this;
    }

    ResultsFilterBuilder score(String fieldName, double threshold) {
        if (threshold > 0.0) {
            RangeQueryBuilder scoreFilter = QueryBuilders.rangeQuery(fieldName);
            scoreFilter.gte(threshold);
            addFilter(scoreFilter);
        }
        return this;
    }

    public ResultsFilterBuilder interim(String fieldName, boolean includeInterim) {
        if (includeInterim) {
            // Including interim results does not stop final results being
            // shown, so including interim results means no filtering on the
            // isInterim field
            return this;
        }

        // Implemented as "NOT isInterim == true" so that not present and null
        // are equivalent to false.  This improves backwards compatibility.
        // Also, note how for a boolean field, unlike numeric term filters, the
        // term value is supplied as a string.
        TermQueryBuilder interimFilter = QueryBuilders.termQuery(fieldName,
                Boolean.TRUE.toString());
        QueryBuilder notInterimFilter = QueryBuilders.boolQuery().mustNot(interimFilter);
        addFilter(notInterimFilter);
        return this;
    }

    ResultsFilterBuilder term(String fieldName, String fieldValue) {
        if (Strings.isNullOrEmpty(fieldName) || Strings.isNullOrEmpty(fieldValue)) {
            return this;
        }

        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(fieldName, fieldValue);
        addFilter(termQueryBuilder);
        return this;
    }

    private void addFilter(QueryBuilder fb) {
        filters.add(fb);
    }

    public QueryBuilder build() {
        if (filters.isEmpty()) {
            return QueryBuilders.matchAllQuery();
        }
        if (filters.size() == 1) {
            return filters.get(0);
        }
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        for (QueryBuilder query : filters) {
            boolQueryBuilder.filter(query);
        }
        return boolQueryBuilder;
    }
}
