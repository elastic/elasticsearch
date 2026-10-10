/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.groundtruth;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;

/**
 * What the data was like when a ground truth was computed, as far as that ground truth is concerned: the documents that
 * the exact search matched, which are those that have the vector and pass the filters of the query. It is taken by the
 * exact search itself, so that it is of the very data that was scanned.
 * <p>
 * It is a fingerprint and not a copy. Every insert, update and delete changes the sequence number of what is stored, so
 * a ground truth whose data state is not the one of the same search today is out of date. Two numbers are used as
 * neither alone tells all: the number of documents tells the deletes of documents and the sum of their sequence numbers
 * tells the updates.
 * <p>
 * The sum is a number with the precision of a double. Below 2^53 it is exact, above it a change of a single sequence
 * number can be smaller than the rounding, and is then not seen: for the very large indices this fingerprint only
 * tells larger changes.
 *
 * @param documents the number of documents that were matched
 * @param seqNoSum  the sum of the sequence numbers of the documents that were matched
 */
public record DataState(long documents, double seqNoSum) {

    /**
     * The name of the aggregation that sums the sequence numbers of the documents that were matched.
     */
    static final String SEQ_NO_SUM = "seq_no_sum";

    /**
     * Above this a double is no longer exact for whole numbers, and the same documents summed in another order can come out a
     * little different.
     */
    private static final double EXACT_LIMIT = 9_007_199_254_740_992.0;

    /**
     * The aggregation that gives the state of the documents that a search matched.
     */
    static AggregationBuilder aggregation() {
        return AggregationBuilders.sum(SEQ_NO_SUM).field(SeqNoFieldMapper.NAME);
    }

    /**
     * A search that tells the state of the data that the exact search of a query is over, without the scan: it only
     * matches the documents that have the vector and pass the filters, and counts them and sums their sequence numbers.
     */
    public static SearchRequest probe(CapturedQuery query) {
        BoolQueryBuilder matched = QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery(query.field()));
        for (QueryBuilder filter : query.filters()) {
            matched.filter(filter);
        }
        SearchSourceBuilder source = new SearchSourceBuilder().query(matched).size(0).trackTotalHits(true).aggregation(aggregation());
        return new SearchRequest(query.indices()).source(source);
    }

    /**
     * What the data was like according to a response of a search that was asked for it, or {@code null} if it does not say.
     */
    @Nullable
    public static DataState of(SearchResponse response) {
        if (response.getAggregations() == null || response.getHits().getTotalHits() == null) {
            return null;
        }
        Sum sum = response.getAggregations().get(SEQ_NO_SUM);
        return sum == null ? null : new DataState(response.getHits().getTotalHits().value(), sum.value());
    }

    /**
     * Whether this is the state of the same data as the other. The sums of very large indices are compared with the
     * rounding that summing them in another order can have.
     */
    public boolean sameAs(DataState other) {
        if (documents != other.documents) {
            return false;
        }
        if (seqNoSum == other.seqNoSum) {
            return true;
        }
        double larger = Math.max(Math.abs(seqNoSum), Math.abs(other.seqNoSum));
        return larger > EXACT_LIMIT && Math.abs(seqNoSum - other.seqNoSum) <= 4 * Math.ulp(larger);
    }
}
