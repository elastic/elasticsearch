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
        }),
            new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents()),
            budget,
            "sampler",
            () -> NOW,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
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
