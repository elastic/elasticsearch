/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.groundtruth;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class DataStateTests extends ESTestCase {

    public void testNeitherTheExactSearchNorTheProbeIsAnsweredFromSomeOfTheShards() {
        CapturedQuery query = query(1);

        assertThat(ExactSearch.request(query).allowPartialSearchResults(), equalTo(false));
        assertThat(DataState.probe(query, 0).allowPartialSearchResults(), equalTo(false));
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

        SearchRequest request = DataState.probe(query, 2);

        assertArrayEquals(new String[] { "a", "b" }, request.indices());
        assertThat("only the shards with this number", request.preference(), equalTo("_shards:2"));
        assertThat("no hits, only what is counted", request.source().size(), equalTo(0));
        BoolQueryBuilder bool = (BoolQueryBuilder) request.source().query();
        assertThat(
            "the vector has to be there, and the filter of the query",
            bool.filter(),
            equalTo(List.of(QueryBuilders.existsQuery("vec"), brand))
        );
        assertThat(bool.must().isEmpty(), equalTo(true));
        List<AggregationBuilder> aggregations = List.copyOf(request.source().aggregations().getAggregatorFactories());
        assertThat(
            "the documents and the sequence numbers of each index",
            aggregations,
            equalTo(
                List.of(
                    AggregationBuilders.terms("by_index")
                        .field("_index")
                        .size(10_000)
                        .subAggregation(AggregationBuilders.sum("seq_no_sum").field("_seq_no"))
                )
            )
        );
    }

    public void testADocumentThatMovesToAnotherShardIsSeenEvenIfTheTotalsAreTheSame() {
        FakeShards before = FakeShards.of(new FakeShards.Shard("idx", 0, 2, 10), new FakeShards.Shard("idx", 1, 1, 4));
        // the document with the sequence number 4 is deleted from the first shard, and another with the number 4 is added to the second:
        // the number of documents is the same and so is the sum of all the sequence numbers, 14
        FakeShards after = FakeShards.of(new FakeShards.Shard("idx", 0, 1, 6), new FakeShards.Shard("idx", 1, 2, 8));

        assertThat("the number of documents is the same", after.state().documents(), equalTo(before.state().documents()));
        assertThat(after.state(), not(equalTo(before.state())));
    }

    public void testAnUpdateThatChangesOnlyASequenceNumberIsSeen() {
        FakeShards before = FakeShards.of(new FakeShards.Shard("idx", 0, 5, 10));
        FakeShards after = FakeShards.of(new FakeShards.Shard("idx", 0, 5, 11));

        assertThat(after.state(), not(equalTo(before.state())));
    }

    private static CapturedQuery query(int k) {
        return new CapturedQuery(new String[] { "idx" }, "vec", new float[] { k }, k, 10, null, null, List.of(), null);
    }
}
