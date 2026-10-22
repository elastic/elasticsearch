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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.FakeShards;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class GroundTruthRunnerTests extends ESTestCase {

    private static final FakeShards SHARDS = FakeShards.of(new FakeShards.Shard("idx", 0, 3, 6), new FakeShards.Shard("idx", 1, 2, 4));

    public void testAChangeDuringTheExactSearchLeavesTheStateOfBeforeItSoThatItLooksOutOfDate() {
        AtomicReference<FakeShards> shards = new AtomicReference<>(SHARDS);
        FakeShards changed = FakeShards.of(new FakeShards.Shard("idx", 0, 3, 6), new FakeShards.Shard("idx", 1, 2, 9));
        GroundTruthRunner runner = new GroundTruthRunner((request, listener) -> {
            if (FakeShards.isProbe(request)) {
                shards.get().accept(request, listener);
            } else {
                shards.set(changed);
                respond(listener, "doc");
            }
        }, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        CapturedQuery query = query(1);

        run(runner, List.of(query));

        assertThat(stored.get(query).dataState(), equalTo(SHARDS.state()));
        assertThat(stored.get(query).dataState(), not(equalTo(changed.state())));
    }

    public void testTheRunOnlyProgressesWhenTheExecutorRunsAndIsCompleteOnceEvenWhenTheSearchesAnswerAtOnce() {
        Deque<Runnable> queue = new ArrayDeque<>();
        AtomicInteger exactSearches = new AtomicInteger();
        GroundTruthRunner runner = new GroundTruthRunner(exactSearchesOver(exactSearches, -1), queue::add);
        List<CapturedQuery> queries = List.of(query(1), query(2), query(3));
        List<GroundTruthRunner.Result> results = new ArrayList<>();

        runner.run(queries, Function.identity(), stored::put, ActionListener.wrap(results::add, e -> fail(e)));

        assertThat("nothing runs on the caller's thread", exactSearches.get(), equalTo(0));
        assertTrue(results.isEmpty());
        while (queue.isEmpty() == false) {
            queue.poll().run();
        }
        assertThat(results, equalTo(List.of(new GroundTruthRunner.Result(3, 0))));
        assertThat(exactSearches.get(), equalTo(3));
        for (int i = 0; i < queries.size(); i++) {
            assertThat(stored.get(queries.get(i)).neighbors().get(0).id(), equalTo("doc" + (i + 1)));
        }
    }

    public void testAQueryWhoseExactSearchFailsHasNoGroundTruthAndTheNextOnesAreStillComputed() {
        GroundTruthRunner runner = new GroundTruthRunner(exactSearchesOver(new AtomicInteger(), 2), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        List<CapturedQuery> queries = List.of(query(1), query(2), query(3));

        GroundTruthRunner.Result result = run(runner, queries);

        assertThat(result, equalTo(new GroundTruthRunner.Result(2, 1)));
        assertThat(stored.get(queries.get(0)).neighbors().get(0).id(), equalTo("doc1"));
        assertNull(stored.get(queries.get(1)));
        assertThat(stored.get(queries.get(2)).neighbors().get(0).id(), equalTo("doc3"));
    }

    public void testTheAnswerOfAnExactSearchThatComesLaterIsWhatIsStored() {
        Deque<Runnable> queue = new ArrayDeque<>();
        Deque<ActionListener<SearchResponse>> waiting = new ArrayDeque<>();
        GroundTruthRunner runner = new GroundTruthRunner((request, listener) -> {
            if (FakeShards.isProbe(request)) {
                SHARDS.accept(request, listener);
            } else {
                waiting.add(listener);
            }
        }, queue::add);
        List<CapturedQuery> queries = List.of(query(1), query(2));
        List<GroundTruthRunner.Result> results = new ArrayList<>();

        runner.run(queries, Function.identity(), stored::put, ActionListener.wrap(results::add, e -> fail(e)));
        for (int i = 0; i < queries.size(); i++) {
            while (queue.isEmpty() == false) {
                queue.poll().run();
            }
            assertThat("the exact search of query " + (i + 1) + " waits for its answer", waiting.size(), equalTo(1));
            assertThat("nothing is stored before the answer", stored.size(), equalTo(i));
            assertTrue(results.isEmpty());
            respond(waiting.poll(), "doc" + (i + 1));
        }
        while (queue.isEmpty() == false) {
            queue.poll().run();
        }

        assertThat(results, equalTo(List.of(new GroundTruthRunner.Result(2, 0))));
        assertThat(stored.get(queries.get(0)).neighbors().get(0).id(), equalTo("doc1"));
        assertThat(stored.get(queries.get(1)).neighbors().get(0).id(), equalTo("doc2"));
    }

    /** Answers the exact searches at once with a hit numbered by the search, and fails the one numbered {@code failing}. */
    private static BiConsumer<SearchRequest, ActionListener<SearchResponse>> exactSearchesOver(AtomicInteger searches, int failing) {
        return (request, listener) -> {
            if (FakeShards.isProbe(request)) {
                SHARDS.accept(request, listener);
            } else if (searches.incrementAndGet() == failing) {
                listener.onFailure(new IllegalStateException("shard failed"));
            } else {
                respond(listener, "doc" + searches.get());
            }
        };
    }

    private final Map<CapturedQuery, GroundTruth> stored = new IdentityHashMap<>();

    private GroundTruthRunner.Result run(GroundTruthRunner runner, List<CapturedQuery> queries) {
        AtomicReference<GroundTruthRunner.Result> result = new AtomicReference<>();
        AtomicInteger notifications = new AtomicInteger();
        runner.run(queries, Function.identity(), stored::put, ActionListener.wrap(r -> {
            notifications.incrementAndGet();
            result.set(r);
        }, e -> fail(e)));
        assertThat("the caller is notified exactly once", notifications.get(), equalTo(1));
        return result.get();
    }

    private static void respond(ActionListener<SearchResponse> listener, String id) {
        SearchHit hit = SearchHit.unpooled(0, id);
        hit.score(1f);
        hit.shard(new SearchShardTarget("node", new ShardId("idx", "_na_", 0), null));
        SearchHits hits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1f);
        SearchResponse response = SearchResponseUtils.successfulResponse(hits);
        hits.decRef();
        try {
            listener.onResponse(response);
        } finally {
            response.decRef();
        }
    }

    private static CapturedQuery query(int k) {
        return new CapturedQuery(new String[] { "idx" }, "vec", new float[] { k }, k, 10, null, null, List.of(), null);
    }
}
