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
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.storage.QuerySamplingIndex;
import org.elasticsearch.xpack.querysampling.storage.SampleRecord;
import org.elasticsearch.xpack.querysampling.storage.SampledQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class GroundTruthWorkerTests extends ESTestCase {

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

    /** Ids of stored documents that the search for the worker's own queries returns, whose source is not a sampled query. */
    private List<String> unreadable = List.of();

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

    public void testItIsScheduledOnlyWhileTheCostRatioIsAboveZero() {
        DeterministicTaskQueue queue = new DeterministicTaskQueue();
        budget.earn(50);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(QuerySamplingSettings.SAMPLING_COST_RATIO));
        worker().start(queue.getThreadPool(), queue.getThreadPool().generic(), clusterSettings);

        queue.runTasksUpToTimeInOrder(queue.getCurrentTimeMillis() + 2 * GroundTruthWorker.INTERVAL.millis());
        assertTrue("a ratio of 0 schedules nothing", sampleSearches.isEmpty());

        clusterSettings.applySettings(Settings.builder().put(QuerySamplingSettings.SAMPLING_COST_RATIO.getKey(), 0.5).build());
        queue.runTasksUpToTimeInOrder(queue.getCurrentTimeMillis() + 2 * GroundTruthWorker.INTERVAL.millis());
        assertFalse("it runs once there is a ratio", sampleSearches.isEmpty());

        clusterSettings.applySettings(Settings.builder().put(QuerySamplingSettings.SAMPLING_COST_RATIO.getKey(), 0).build());
        sampleSearches.clear();
        queue.runTasksUpToTimeInOrder(queue.getCurrentTimeMillis() + 2 * GroundTruthWorker.INTERVAL.millis());
        assertTrue("and stops when the ratio is 0 again", sampleSearches.isEmpty());
    }

    public void testAStoredQueryThatCannotBeReadIsTouchedSoThatItMakesWayForOthersAndCountsAsFailed() {
        waiting = 2;
        unreadable = List.of("sampler_unreadable");
        budget.earn(50);
        GroundTruthWorker worker = worker();

        worker.run();

        List<DocWriteRequest<?>> updates = bulks.get(0).requests();
        assertThat(updates.size(), equalTo(3));
        UpdateRequest touch = (UpdateRequest) updates.get(2);
        assertThat(touch.id(), equalTo("sampler_unreadable"));
        assertThat("only marked as looked at", touch.doc().sourceAsMap().keySet(), equalTo(Set.of("updated_at")));
        assertThat(worker.computed(), equalTo(2L));
        assertThat(worker.failed(), equalTo(1L));
    }

    private GroundTruthWorker worker() {
        return new GroundTruthWorker((request, listener) -> {
            sampleSearches.add(request);
            try {
                boolean own = request.source().query().toString().contains("sampler_id");
                respondWithPending(listener, own ? Math.min(waiting, request.source().size()) : 0, own ? unreadable : List.of());
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
        }),
            new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents()),
            budget,
            "sampler",
            () -> NOW,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    private void respondWithPending(ActionListener<SearchResponse> listener, int count, List<String> unreadableIds) throws IOException {
        SearchHit[] hits = new SearchHit[count + unreadableIds.size()];
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
        for (int i = 0; i < unreadableIds.size(); i++) {
            hits[count + i] = SearchHit.unpooled(count + i, unreadableIds.get(i));
            hits[count + i].sourceRef(new BytesArray("{\"query\":\"not a query\"}"));
        }
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1f);
        SearchResponse response = SearchResponseUtils.successfulResponse(searchHits);
        searchHits.decRef();
        try {
            listener.onResponse(response);
        } finally {
            response.decRef();
        }
        waiting -= count;
    }
}
