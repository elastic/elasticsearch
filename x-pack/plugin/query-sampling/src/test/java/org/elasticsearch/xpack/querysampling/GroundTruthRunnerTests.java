/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.storage.SampledQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class GroundTruthRunnerTests extends ESTestCase {

    public void testComputesTheGroundTruthOfEveryQuery() {
        List<SampledQuery> queries = List.of(sampled(1), sampled(2), sampled(3));
        // each test query asks for as many neighbours as its number, which is how the fake search tells them apart
        GroundTruthRunner runner = new GroundTruthRunner(answering(request -> "doc" + request.source().size()));

        GroundTruthRunner.Result result = run(runner, queries);

        assertThat(result, equalTo(new GroundTruthRunner.Result(3, 0)));
        for (int i = 0; i < 3; i++) {
            assertThat(queries.get(i).groundTruth().neighbors(), equalTo(List.of(new CapturedSearch.Hit("idx", "doc" + (i + 1), 1f))));
        }
    }

    public void testQueriesAreProcessedOneAfterTheOther() {
        List<SampledQuery> queries = List.of(sampled(1), sampled(2), sampled(3));
        List<Runnable> inFlight = new ArrayList<>();
        // searches only complete when the test says so
        GroundTruthRunner runner = new GroundTruthRunner(
            (request, listener) -> inFlight.add(() -> respond(listener, "doc" + request.source().size()))
        );
        AtomicReference<GroundTruthRunner.Result> result = new AtomicReference<>();

        runner.run(queries, ActionListener.wrap(result::set, e -> fail(e)));

        for (int expectedComputed = 0; expectedComputed < 3; expectedComputed++) {
            assertThat("a search is only started when the previous one is done", inFlight.size(), equalTo(expectedComputed + 1));
            assertThat(result.get(), nullValue());
            inFlight.get(expectedComputed).run();
        }
        assertThat(result.get(), equalTo(new GroundTruthRunner.Result(3, 0)));
    }

    public void testFailedSearchesAreCountedAndLeaveTheQueryPending() {
        SampledQuery failing = sampled(1);
        SampledQuery throwing = sampled(2);
        SampledQuery working = sampled(3);
        GroundTruthRunner runner = new GroundTruthRunner((request, listener) -> {
            switch (request.source().size()) {
                case 1 -> listener.onFailure(new IllegalStateException("search failed"));
                case 2 -> throw new IllegalStateException("could not be sent");
                case 3 -> respond(listener, "doc3");
                default -> throw new AssertionError("unexpected query");
            }
        });

        GroundTruthRunner.Result result = run(runner, List.of(failing, throwing, working));

        assertThat(result, equalTo(new GroundTruthRunner.Result(1, 2)));
        assertThat(failing.groundTruth(), nullValue());
        assertThat(throwing.groundTruth(), nullValue());
        assertThat(working.groundTruth().neighbors().size(), equalTo(1));
    }

    public void testSearchThatCompletesAndThenThrowsIsOnlyCountedOnce() {
        SampledQuery query = sampled(1);
        GroundTruthRunner runner = new GroundTruthRunner((request, listener) -> {
            respond(listener, "doc1");
            throw new IllegalStateException("thrown after the response");
        });

        assertThat(run(runner, List.of(query)), equalTo(new GroundTruthRunner.Result(1, 0)));
    }

    public void testNothingToDo() {
        AtomicInteger searches = new AtomicInteger();
        GroundTruthRunner runner = new GroundTruthRunner((request, listener) -> searches.incrementAndGet());

        assertThat(run(runner, List.of()), equalTo(new GroundTruthRunner.Result(0, 0)));
        assertThat(searches.get(), equalTo(0));
    }

    private static GroundTruthRunner.Result run(GroundTruthRunner runner, List<SampledQuery> queries) {
        AtomicReference<GroundTruthRunner.Result> result = new AtomicReference<>();
        AtomicInteger notifications = new AtomicInteger();
        runner.run(queries, ActionListener.wrap(r -> {
            notifications.incrementAndGet();
            result.set(r);
        }, e -> fail(e)));
        assertThat("the caller is notified exactly once", notifications.get(), equalTo(1));
        return result.get();
    }

    /**
     * A search function that answers every exact search with a single hit whose id is derived from the request.
     */
    private static BiConsumer<SearchRequest, ActionListener<SearchResponse>> answering(Function<SearchRequest, String> id) {
        return (request, listener) -> respond(listener, id.apply(request));
    }

    private static void respond(ActionListener<SearchResponse> listener, String id) {
        SearchHit hit = SearchHit.unpooled(0, id);
        hit.score(1f);
        hit.shard(new SearchShardTarget("node", new ShardId("idx", "_na_", 0), null));
        SearchHits hits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1f);
        SearchResponse response = SearchResponseUtils.successfulResponse(hits);
        hits.decRef(); // the response holds its own reference
        try {
            listener.onResponse(response);
        } finally {
            response.decRef();
        }
    }

    private static SampledQuery sampled(float value) {
        int k = (int) value;
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", new float[] { value }, k, 10, null, null, List.of(), null);
        return new SampledQuery(new QueryFingerprint(k, 0), new CapturedSearch(query, List.of(), 1), new TrackedQuery());
    }
}
