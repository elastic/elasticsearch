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
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
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

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class GroundTruthWorkerTests extends ESTestCase {

    private static final long NOW = 100_000_000L;

    private final List<SearchRequest> sampleSearches = new ArrayList<>();
    private final List<SearchRequest> exactSearches = new ArrayList<>();
    private final List<BulkRequest> bulks = new ArrayList<>();
    private final CostBudget budget = new CostBudget(0.5, 1_000_000);
    /** How many stored queries wait for the next search of the index of the sample. */
    private int waiting;
    private ActionListener<SearchResponse> heldSampleSearch;
    private boolean holdSampleSearch;
    private boolean failSampleSearch;

    public void testNothingIsComputedWithoutCredit() {
        waiting = 5;
        GroundTruthWorker worker = worker();

        worker.run();

        assertThat(sampleSearches.size(), equalTo(0));
    }

    public void testNothingIsComputedWithoutARatioHoweverMuchCreditThereIs() {
        waiting = 5;
        budget.earn(1000);
        budget.ratio(0.0);
        GroundTruthWorker worker = worker();

        worker.run();

        assertThat(sampleSearches.size(), equalTo(0));
    }

    public void testItComputesWhatTheCreditAffords() {
        waiting = 5;
        budget.earn(50); // 25 ms of credit, and a search is taken to cost 10 ms until it is known better
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
        assertThat("each of the two searches took 5 ms", budget.credit(), closeTo(25 - 10, 1e-9));
    }

    public void testWhenItsOwnAreDoneItTakesWhatNobodyLooksAfter() {
        waiting = 0;
        budget.earn(1000);
        GroundTruthWorker worker = worker();

        worker.run();

        assertThat(sampleSearches.size(), equalTo(2));
        assertThat(
            sampleSearches.get(1).source().query(),
            equalTo(
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("has_ground_truth", false))
                    .filter(QueryBuilders.rangeQuery("updated_at").lt(NOW - GroundTruthWorker.ABANDONED_AFTER.millis()))
            )
        );
    }

    public void testOnlyOneBatchIsComputedAtATime() {
        waiting = 5;
        holdSampleSearch = true;
        budget.earn(1000);
        GroundTruthWorker worker = worker();

        worker.run();
        worker.run();

        assertThat("the second round finds the first still running", sampleSearches.size(), equalTo(1));
        holdSampleSearch = false;
        heldSampleSearch.onFailure(new IllegalStateException("search failed"));
        worker.run();
        assertThat("and it is free to work once the first is over", sampleSearches.size(), equalTo(2));
    }

    public void testAFailureDoesNotStopIt() {
        waiting = 5;
        failSampleSearch = true;
        budget.earn(1000);
        GroundTruthWorker worker = worker();

        worker.run();
        failSampleSearch = false;
        worker.run();

        assertThat(worker.computed(), equalTo(5L));
    }

    public void testItLooksForWorkRegularlyUntilStopped() {
        waiting = 1;
        budget.earn(1000);
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        GroundTruthWorker worker = worker();

        var cancellable = worker.start(taskQueue.getThreadPool(), taskQueue.getThreadPool().generic());
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertThat(worker.computed(), equalTo(1L));

        cancellable.cancel();
        waiting = 1;
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertThat(worker.computed(), equalTo(1L));
    }

    private GroundTruthWorker worker() {
        return new GroundTruthWorker((request, listener) -> {
            sampleSearches.add(request);
            if (failSampleSearch) {
                listener.onFailure(new IllegalStateException("search failed"));
            } else if (holdSampleSearch) {
                heldSampleSearch = listener;
            } else {
                try {
                    // the query of the owner is answered with what waits, the rest with nothing
                    boolean own = request.source().query().toString().contains("sampler_id");
                    respondWithPending(listener, own ? Math.min(waiting, request.source().size()) : 0);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        }, (request, listener) -> {
            bulks.add(request);
            BulkItemResponse[] items = new BulkItemResponse[request.numberOfActions()];
            for (int i = 0; i < items.length; i++) {
                items[i] = BulkItemResponse.success(
                    i,
                    DocWriteRequest.OpType.UPDATE,
                    new UpdateResponse(new ShardId(QuerySamplingIndex.NAME, "_na_", 0), "id", 0, 1, 1, UpdateResponse.Result.UPDATED)
                );
            }
            listener.onResponse(new BulkResponse(items, 1));
        }, (request, listener) -> {
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
        }, new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents()), budget, "sampler", () -> NOW);
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
}
