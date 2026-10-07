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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class StoredGroundTruthTests extends ESTestCase {

    private final List<SearchRequest> sampleSearches = new ArrayList<>();
    private final List<SearchRequest> exactSearches = new ArrayList<>();
    private final List<BulkRequest> bulks = new ArrayList<>();

    public void testGroundTruthIsComputedForWhatIsPendingAndStoredWithIt() throws IOException {
        StoredGroundTruth service = service(pending(1, 2), failingEveryExactSearchOf(-1), acknowledgingAll());

        GroundTruthRunner.Result result = compute(service, 5);

        assertThat(result, equalTo(new GroundTruthRunner.Result(2, 0)));
        SearchRequest sampleSearch = sampleSearches.get(0);
        assertThat(sampleSearch.indices(), equalTo(new String[] { QuerySamplingIndex.NAME }));
        assertThat(sampleSearch.source().size(), equalTo(5));
        assertThat(sampleSearch.source().query(), equalTo(QueryBuilders.termQuery("has_ground_truth", false)));

        assertThat("the stored queries are searched", exactSearches.stream().map(r -> r.source().size()).toList(), equalTo(List.of(1, 2)));
        assertThat(bulks.size(), equalTo(1));
        List<DocWriteRequest<?>> updates = bulks.get(0).requests();
        assertThat(updates.size(), equalTo(2));
        assertThat(updates.get(0).id(), equalTo(SampleRecord.documentId("sampler", new QueryFingerprint(1, 1))));
        assertThat(updates.get(0), instanceOf(UpdateRequest.class));
        String update = ((UpdateRequest) updates.get(0)).doc().source().utf8ToString();
        assertThat(update.contains("\"has_ground_truth\":true"), equalTo(true));
        assertThat(update.contains("\"neighbors\""), equalTo(true));
    }

    public void testAFailedSearchLeavesTheQueryPendingButMovesItBack() throws IOException {
        StoredGroundTruth service = service(pending(1, 2), failingEveryExactSearchOf(1), acknowledgingAll());

        GroundTruthRunner.Result result = compute(service, 5);

        assertThat(result, equalTo(new GroundTruthRunner.Result(1, 1)));
        List<DocWriteRequest<?>> updates = bulks.get(0).requests();
        String failed = ((UpdateRequest) updates.get(0)).doc().source().utf8ToString();
        assertThat("only marked as looked at", failed.contains("has_ground_truth"), equalTo(false));
        assertThat(failed.contains("updated_at"), equalTo(true));
        assertThat(((UpdateRequest) updates.get(1)).doc().source().utf8ToString().contains("\"has_ground_truth\":true"), equalTo(true));
    }

    public void testNothingIsDoneWhenNothingIsPending() throws IOException {
        StoredGroundTruth service = service(pending(), failingEveryExactSearchOf(-1), acknowledgingAll());

        assertThat(compute(service, 5), equalTo(new GroundTruthRunner.Result(0, 0)));
        assertThat(exactSearches.size(), equalTo(0));
        assertThat(bulks.size(), equalTo(0));
    }

    public void testNothingWasEverSampled() {
        StoredGroundTruth service = service(
            (request, listener) -> listener.onFailure(new IndexNotFoundException(QuerySamplingIndex.NAME)),
            failingEveryExactSearchOf(-1),
            acknowledgingAll()
        );

        assertThat(compute(service, 5), equalTo(new GroundTruthRunner.Result(0, 0)));
    }

    public void testAFailedBulkRequestIsReported() throws IOException {
        StoredGroundTruth service = service(pending(1), failingEveryExactSearchOf(-1), (request, listener) -> {
            listener.onFailure(new IllegalStateException("bulk failed"));
        });
        AtomicReference<Exception> failure = new AtomicReference<>();

        service.compute(5, ActionListener.wrap(result -> fail("expected a failure"), failure::set));

        assertThat(failure.get(), instanceOf(IllegalStateException.class));
    }

    public void testARejectedUpdateIsNotCounted() throws IOException {
        StoredGroundTruth service = service(pending(1, 2), failingEveryExactSearchOf(-1), (request, listener) -> {
            bulks.add(request);
            BulkItemResponse rejected = BulkItemResponse.failure(
                0,
                DocWriteRequest.OpType.UPDATE,
                new BulkItemResponse.Failure(QuerySamplingIndex.NAME, "id", new IllegalStateException("rejected"))
            );
            listener.onResponse(new BulkResponse(new BulkItemResponse[] { rejected, success(1) }, 1));
        });

        assertThat(compute(service, 5), equalTo(new GroundTruthRunner.Result(1, 1)));
    }

    private GroundTruthRunner.Result compute(StoredGroundTruth service, int max) {
        AtomicReference<GroundTruthRunner.Result> result = new AtomicReference<>();
        service.compute(max, ActionListener.wrap(result::set, e -> fail(e)));
        return result.get();
    }

    private StoredGroundTruth service(
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> sampleSearch,
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> exactSearch,
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulk
    ) {
        return new StoredGroundTruth((request, listener) -> {
            sampleSearches.add(request);
            sampleSearch.accept(request, listener);
        }, bulk, (request, listener) -> {
            exactSearches.add(request);
            exactSearch.accept(request, listener);
        }, new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents()), () -> 42L);
    }

    /**
     * The stored queries with these numbers, answered to the search of the pending ones. A query with the number
     * {@code n} asks for {@code n} neighbours, which is how the fake exact search tells them apart.
     */
    private static BiConsumer<SearchRequest, ActionListener<SearchResponse>> pending(int... numbers) throws IOException {
        SearchHit[] hits = new SearchHit[numbers.length];
        for (int i = 0; i < numbers.length; i++) {
            long n = numbers[i];
            CapturedQuery query = new CapturedQuery(
                new String[] { "idx" },
                "vec",
                new float[] { n },
                (int) n,
                10,
                null,
                null,
                List.of(),
                null
            );
            QueryFingerprint fingerprint = new QueryFingerprint(n, n);
            SampledQuery sampled = new SampledQuery(
                fingerprint,
                new CapturedSearch(query, List.of(), 1, 1.0),
                new MultiplicityTracker(10).record(fingerprint)
            );
            hits[i] = SearchHit.unpooled(i, SampleRecord.documentId("sampler", fingerprint));
            hits[i].sourceRef(BytesReference.bytes(SampleRecord.document(JsonXContent.contentBuilder(), "sampler", sampled, 1L)));
        }
        return (request, listener) -> {
            SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1f);
            SearchResponse response = SearchResponseUtils.successfulResponse(searchHits);
            searchHits.decRef();
            try {
                listener.onResponse(response);
            } finally {
                response.decRef();
            }
        };
    }

    /**
     * Answers every exact search with one neighbour, except those that ask for {@code failing} neighbours.
     */
    private static BiConsumer<SearchRequest, ActionListener<SearchResponse>> failingEveryExactSearchOf(int failing) {
        return (request, listener) -> {
            if (request.source().size() == failing) {
                listener.onFailure(new IllegalStateException("search failed"));
                return;
            }
            SearchHit hit = SearchHit.unpooled(0, "doc");
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
        };
    }

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
}
