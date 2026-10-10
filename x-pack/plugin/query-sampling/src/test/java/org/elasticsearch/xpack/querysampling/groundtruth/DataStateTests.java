/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.groundtruth;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DataStateTests extends ESTestCase {

    public void testTheSameDataIsTheSameState() {
        assertTrue(new DataState(5, 10.0).sameAs(new DataState(5, 10.0)));
    }

    public void testAChangeOfTheNumberOfDocumentsIsSeen() {
        assertFalse(new DataState(5, 10.0).sameAs(new DataState(6, 10.0)));
    }

    public void testAnUpdateThatChangesOnlyASequenceNumberIsSeen() {
        assertFalse("one document was updated: the same number of them", new DataState(5, 10.0).sameAs(new DataState(5, 14.0)));
        assertFalse("as small a change as there can be", new DataState(5, 10.0).sameAs(new DataState(5, 11.0)));
    }

    public void testRoundingOfVeryLargeSumsIsNotAChange() {
        double large = 4e18;

        assertTrue("summed in another order", new DataState(5, large).sameAs(new DataState(5, large + Math.ulp(large))));
        assertFalse("a real change is far more than the rounding", new DataState(5, large).sameAs(new DataState(5, large + 1e9)));
    }

    public void testSmallSumsAreExact() {
        double sum = 9_000_000_000_000_000.0; // under 2^53, where every whole number is there

        assertFalse(new DataState(5, sum).sameAs(new DataState(5, sum + 2)));
    }

    public void testProbeMatchesWhatTheExactSearchScansAndOnlyCountsIt() {
        QueryBuilder brand = QueryBuilders.termQuery("brand", "apple");
        CapturedQuery query = new CapturedQuery(
            new String[] { "a", "b" },
            "vec",
            new float[] { 1f },
            7,
            100,
            null,
            null,
            List.of(brand),
            "q1"
        );

        SearchRequest request = DataState.probe(query);

        assertArrayEquals(new String[] { "a", "b" }, request.indices());
        assertThat("no hits, only what is counted", request.source().size(), equalTo(0));
        assertThat(request.source().trackTotalHitsUpTo(), equalTo(SearchContext.TRACK_TOTAL_HITS_ACCURATE));
        BoolQueryBuilder bool = (BoolQueryBuilder) request.source().query();
        assertThat(
            "the vector has to be there, and the filter of the query",
            bool.filter(),
            equalTo(List.of(QueryBuilders.existsQuery("vec"), brand))
        );
        assertThat(bool.must().isEmpty(), equalTo(true));
        List<AggregationBuilder> aggregations = List.copyOf(request.source().aggregations().getAggregatorFactories());
        assertThat(aggregations, equalTo(List.of(AggregationBuilders.sum("seq_no_sum").field("_seq_no"))));
    }

    public void testStateOfAResponse() {
        SearchHits searchHits = new SearchHits(new SearchHit[0], new TotalHits(42, TotalHits.Relation.EQUAL_TO), 1f);
        SearchResponse response = SearchResponseUtils.response(searchHits)
            .aggregations(InternalAggregations.from(List.of(new Sum("seq_no_sum", 1234.0, DocValueFormat.RAW, Map.of()))))
            .build();
        searchHits.decRef(); // the response holds its own reference
        try {
            assertThat(DataState.of(response), equalTo(new DataState(42, 1234.0)));
        } finally {
            response.decRef();
        }
    }

    public void testAResponseThatDoesNotSayItHasNoState() {
        SearchHits searchHits = new SearchHits(new SearchHit[0], new TotalHits(42, TotalHits.Relation.EQUAL_TO), 1f);
        SearchResponse response = SearchResponseUtils.successfulResponse(searchHits);
        searchHits.decRef();
        try {
            assertThat(DataState.of(response), nullValue());
        } finally {
            response.decRef();
        }
    }
}
