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

/**
 * Ranged or boxed query. Provides a beginning or end to the current query.
 * The query moves between them through search_after.
 * 
 * Note that the range is not set at once on purpose since each query tends to have
 * its own number of results separate from the others.
 * As such, each query starts where it lefts to reach the current in-progress window
 * as oppose to always operating with the exact same window.
 */
public class BoxedQueryRequest implements QueryRequest {

    private final RangeQueryBuilder timestampRange;
    private final RangeQueryBuilder tiebreakerRange;

    private final SearchSourceBuilder searchSource;

    private Ordinal from, to;
    private Ordinal after;

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
        after = ordinal;
        // and leave only search_after
        searchSource.searchAfter(ordinal.toArray());
    }

    /**
     * Sets the lower boundary for the query (inclusive).
     * Can be removed (when the query in unbounded) through null.
     */
    public BoxedQueryRequest from(Ordinal begin) {
        from = begin;
        timestampRange.gte(begin != null ? begin.timestamp() : null);
        if (tiebreakerRange != null) {
            tiebreakerRange.gte(begin != null ? begin.tiebreaker() : null);
        }
        return this;
    }

    public Ordinal after() {
        return after;
    }

    public Ordinal from() {
        return from;
    }

    /**
     * Sets the upper boundary for the query (inclusive).
     * Can be removed (when the query in unbounded) through null.
     */
    public BoxedQueryRequest to(Ordinal end) {
        to = end;
        timestampRange.lte(end != null ? end.timestamp() : null);
        if (tiebreakerRange != null) {
            tiebreakerRange.lte(end != null ? end.tiebreaker() : null);
        }
        return this;
    }

    @Override
    public String toString() {
        return "( " + string(from) + " >-" + string(after) + "-> " + string(to) + "]";
    }

    private static String string(Ordinal o) {
        return o != null ? o.toString() : "<none>";
    }
}