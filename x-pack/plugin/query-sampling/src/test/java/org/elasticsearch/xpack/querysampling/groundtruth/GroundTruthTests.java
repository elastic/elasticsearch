/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.groundtruth;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.querysampling.GroundTruthRunner;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.storage.QuerySamplingIndex;
import org.elasticsearch.xpack.querysampling.storage.SampleRecord;
import org.elasticsearch.xpack.querysampling.storage.SampledQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class GroundTruthTests extends ESTestCase {

    private static final FakeShards SHARDS = FakeShards.of(new FakeShards.Shard("idx", 0, 3, 6), new FakeShards.Shard("idx", 1, 2, 4));

    /** Answers the state probes from the shards and leaves the exact searches to the given function. */
    private static BiConsumer<SearchRequest, ActionListener<SearchResponse>> overShards(
        FakeShards shards,
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> search
    ) {
        return (request, listener) -> {
            if (FakeShards.isProbe(request)) {
                shards.accept(request, listener);
            } else {
                search.accept(request, listener);
            }
        };
    }

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
        });
        CapturedQuery query = query(1);

        run(runner, List.of(query));

        assertThat(stored.get(query).dataState(), equalTo(SHARDS.state()));
        assertThat(stored.get(query).dataState(), not(equalTo(changed.state())));
    }

    private final List<SearchRequest> sampleSearches = new ArrayList<>();

    private final List<SearchRequest> exactSearches = new ArrayList<>();

    private final List<BulkRequest> bulks = new ArrayList<>();

    private BiConsumer<BulkRequest, ActionListener<BulkResponse>> acknowledgingAll() {
        return (request, listener) -> {
            bulks.add(request);
            BulkItemResponse[] items = new BulkItemResponse[request.numberOfActions()];
            for (int i = 0; i < items.length; i++) {
                items[i] = success(i);
            }
            listener.onResponse(new BulkResponse(items, 1));
        };
    }

    private static BulkItemResponse success(int id) {
        return BulkItemResponse.success(
            id,
            DocWriteRequest.OpType.UPDATE,
            new UpdateResponse(new ShardId(QuerySamplingIndex.NAME, "_na_", 0), "id", 0, 1, 1, DocWriteResponse.Result.UPDATED)
        );
    }

    private static final long NOW = 100_000_000L;

    private final CostBudget budget = new CostBudget(0.5, 1_000_000);

    private int waiting;

    public void testItComputesWhatTheCreditAffords() {
        waiting = 5;
        budget.earn(50);
        GroundTruthWorker worker = worker();

        worker.run();

        assertThat("room for two", sampleSearches.get(0).source().size(), equalTo(2));
        assertThat(
            "of its own",
            sampleSearches.get(0).source().query(),
            equalTo(
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("has_ground_truth", false))
                    .filter(QueryBuilders.termQuery("sampler_id", "sampler"))
            )
        );
        assertThat(exactSearches.size(), equalTo(2));
        assertThat(bulks.size(), equalTo(1));
        assertThat(worker.computed(), equalTo(2L));
        assertThat(
            "each of the two searches took 5 ms and each of the 6 reads of the state of the data 1 ms",
            budget.credit(),
            closeTo(25 - 10 - 6, 1e-9)
        );
    }

    private GroundTruthWorker worker() {
        return new GroundTruthWorker((request, listener) -> {
            sampleSearches.add(request);
            try {
                boolean own = request.source().query().toString().contains("sampler_id");
                respondWithPending(listener, own ? Math.min(waiting, request.source().size()) : 0);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }, acknowledgingAll(), overShards(SHARDS, (request, listener) -> {
            exactSearches.add(request);
            SearchHit hit = SearchHit.unpooled(0, "doc");
            hit.score(1f);
            hit.shard(new SearchShardTarget("node", new ShardId("idx", "_na_", 0), null));
            SearchHits hits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1f);
            SearchResponse response = SearchResponseUtils.response(hits).tookInMillis(5).build();
            hits.decRef();
            try {
                listener.onResponse(response);
            } finally {
                response.decRef();
            }
        }), new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents()), budget, "sampler", () -> NOW);
    }

    private void respondWithPending(ActionListener<SearchResponse> listener, int count) throws IOException {
        SearchHit[] hits = new SearchHit[count];
        for (int i = 0; i < count; i++) {
            QueryFingerprint fingerprint = new QueryFingerprint(i + 1, i + 1);
            CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", new float[] { i }, 3, 10, null, null, List.of(), null);
            SampledQuery sampled = new SampledQuery(
                fingerprint,
                new CapturedSearch(query, List.of(), 1, 1.0),
                new MultiplicityTracker(10).record(fingerprint)
            );
            hits[i] = SearchHit.unpooled(i, SampleRecord.documentId("sampler", fingerprint));
            hits[i].sourceRef(BytesReference.bytes(SampleRecord.document(JsonXContent.contentBuilder(), "sampler", sampled, 1L)));
        }
        SearchHits searchHits = new SearchHits(hits, new TotalHits(count, TotalHits.Relation.EQUAL_TO), 1f);
        SearchResponse response = SearchResponseUtils.successfulResponse(searchHits);
        searchHits.decRef();
        try {
            listener.onResponse(response);
        } finally {
            response.decRef();
        }
        waiting -= count;
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
