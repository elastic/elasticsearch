/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.eql.execution.search.Ordinal;
import org.elasticsearch.xpack.eql.execution.search.QueryRequest;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

public class BoxedQueryRequest implements QueryRequest {

    private final RangeQueryBuilder timestampRange;
    private final RangeQueryBuilder tiebreakerRange;

    private final SearchSourceBuilder searchSource;

    public BoxedQueryRequest(QueryRequest original, String timestamp, String tiebreaker) {
        searchSource = original.searchSource();

        // setup range queries and preserve their reference to simplify the update
        timestampRange = rangeQuery(timestamp).timeZone("UTC").format("epoch_millis");
        BoolQueryBuilder filter = boolQuery().filter(timestampRange);
        if (tiebreaker != null) {
            tiebreakerRange = rangeQuery(tiebreaker);
            filter.filter(tiebreakerRange);
        } else {
            tiebreakerRange = null;
        }
        // add ranges to existing query
        searchSource.query(filter.must(searchSource.query()));
    }

    @Override
    public SearchSourceBuilder searchSource() {
        return searchSource;
    }

    @Override
    public void nextAfter(Ordinal ordinal) {
        if (tiebreakerRange != null) {
            tiebreakerRange.gt(ordinal.tiebreaker());
            timestampRange.gte(ordinal.timestamp());
        } else {
            timestampRange.gt(ordinal.timestamp());
        }
        // and leave only search_after
        searchSource.searchAfter(ordinal.toArray());
    }

    /**
     * Sets the upper boundary for the query. Can be removed (when the query in unbounded) through null.
     */
    public BoxedQueryRequest until(Ordinal end) {
        timestampRange.lte(end != null ? end.timestamp() : null);
        if (tiebreakerRange != null) {
            tiebreakerRange.lte(end != null ? end.tiebreaker() : null);
        }
        return this;
    }

    @Override
    public String toString() {
        return searchSource.toString();
    }
}