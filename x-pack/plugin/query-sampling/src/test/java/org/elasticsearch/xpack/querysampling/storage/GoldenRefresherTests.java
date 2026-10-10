/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.DataState;
import org.elasticsearch.xpack.querysampling.groundtruth.ExactSearch;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class GoldenRefresherTests extends ESTestCase {

    private static final NamedXContentRegistry REGISTRY = new NamedXContentRegistry(
        new SearchModule(Settings.EMPTY, List.of()).getNamedXContents()
    );
    private static final DataState STORED = new DataState(5, 10.0);
    private static final DataState CHANGED = new DataState(6, 15.0);

    private final List<SearchRequest> probes = new ArrayList<>();
    private final List<SearchRequest> exactSearches = new ArrayList<>();
    private final List<IndexRequest> manifests = new ArrayList<>();
    private final List<BulkRequest> bulks = new ArrayList<>();
    private final List<UpdateRequest> updates = new ArrayList<>();

    private List<SearchHit> records = List.of();
    private long latestVersion = 4; // the latest that exists, which is also the latest that is complete
    private Function<Long, DataState> now = n -> STORED; // the state of the data of the query of the record n, as the probe tells it
    private java.util.Set<Long> failingProbes = java.util.Set.of();
    private java.util.Set<Long> failingExact = java.util.Set.of();

    private GoldenRefresher refresher() {
        BiConsumerOfSearch golden = (request, listener) -> {
            if (request.source().size() == 1) {
                respond(listener, latestVersion == 0 ? List.of() : List.of(manifest(latestVersion)), null);
            } else {
                respond(listener, records, null);
            }
        };
        GoldenReader reader = new GoldenReader(golden::accept, REGISTRY);
        GoldenStaleness staleness = new GoldenStaleness(reader, (request, listener) -> {
            probes.add(request);
            long n = numberOf(request);
            if (failingProbes.contains(n)) {
                listener.onFailure(new IllegalStateException("no"));
            } else {
                respond(listener, List.of(), now.apply(n));
            }
        });
        GoldenWriter writer = new GoldenWriter(golden::accept, (request, listener) -> {
            manifests.add(request);
            listener.onResponse(new IndexResponse(new ShardId(GoldenIndex.NAME, "_na_", 0), request.id(), 0, 1, 1, true));
        }, (request, listener) -> {
            bulks.add(request);
            BulkItemResponse[] items = new BulkItemResponse[request.numberOfActions()];
            for (int i = 0; i < items.length; i++) {
                items[i] = BulkItemResponse.success(
                    i,
                    DocWriteRequest.OpType.CREATE,
                    new IndexResponse(new ShardId(GoldenIndex.NAME, "_na_", 0), "id", 0, 1, 1, true)
                );
            }
            listener.onResponse(new BulkResponse(items, 1));
        }, (request, listener) -> {
            updates.add(request);
            listener.onResponse(
                new UpdateResponse(new ShardId(GoldenIndex.NAME, "_na_", 0), request.id(), 0, 1, 1, DocWriteResponse.Result.UPDATED)
            );
        }, () -> 42L);
        return new GoldenRefresher(reader, staleness, (request, listener) -> {
            exactSearches.add(request);
            long n = numberOf(request);
            if (failingExact.contains(n)) {
                listener.onFailure(new IllegalStateException("no"));
                return;
            }
            SearchHit hit = SearchHit.unpooled(0, "fresh" + n);
            hit.score(1f);
            hit.shard(new SearchShardTarget("node", new ShardId("idx" + n, "_na_", 0), null));
            respond(listener, List.of(hit), CHANGED);
        }, writer);
    }

    /**
     * What the fake search of the golden dataset takes.
     */
    private interface BiConsumerOfSearch {
        void accept(SearchRequest request, ActionListener<SearchResponse> listener);
    }

    private static long numberOf(SearchRequest request) {
        return Long.parseLong(request.indices()[0].substring(3));
    }

    private GoldenRefresher.Result refresh(GoldenRefresher refresher, long version, int max) {
        AtomicReference<GoldenRefresher.Result> result = new AtomicReference<>();
        refresher.refresh(version, max, ActionListener.wrap(result::set, e -> fail(e)));
        return result.get();
    }

    public void testFreshQueriesAreCopiedAndStaleOnesAreComputedAgainInANewVersion() throws IOException {
        records = List.of(record(1, STORED), record(2, STORED), record(3, STORED));
        now = n -> n == 2 ? CHANGED : STORED;

        GoldenRefresher.Result result = refresh(refresher(), 0, 10);

        assertThat(result, equalTo(new GoldenRefresher.Result(4, 5, 2, 1, 0)));
        assertThat("only the stale one is scanned again", exactSearches.size(), equalTo(1));
        assertThat(exactSearches.get(0).source().size(), equalTo(3));
        assertThat(manifests.get(0).id(), equalTo("version_5"));
        assertThat(
            "the manifest says what it was made from",
            manifests.get(0).source().utf8ToString().contains("\"source_version\":4"),
            equalTo(true)
        );
        assertThat(bulks.get(0).numberOfActions(), equalTo(3));
        assertThat(
            bulks.get(0).requests().stream().map(DocWriteRequest::id).toList(),
            equalTo(List.of("5_" + "1", "5_" + "2", "5_" + "3"))
        );
        String refreshed = ((IndexRequest) bulks.get(0).requests().get(1)).source().utf8ToString();
        assertThat(
            "the new ground truth and the state of the data it was computed on",
            refreshed.contains("\"id\":\"fresh2\""),
            equalTo(true)
        );
        assertThat(refreshed.contains("\"documents\":6"), equalTo(true));
        String copied = ((IndexRequest) bulks.get(0).requests().get(0)).source().utf8ToString();
        assertThat("as it was", copied.contains("\"documents\":5"), equalTo(true));
        assertThat(updates.get(0).doc().source().utf8ToString(), equalTo("{\"records\":3,\"completed\":true}"));
    }

    public void testNoVersionIsMadeWhenEverythingIsFresh() throws IOException {
        records = List.of(record(1, STORED), record(2, STORED));

        GoldenRefresher.Result result = refresh(refresher(), 4, 10);

        assertThat(result, equalTo(new GoldenRefresher.Result(4, 0, 2, 0, 0)));
        assertThat("a copy of the version there is would be of no use", manifests.size(), equalTo(0));
        assertThat(bulks.size(), equalTo(0));
        assertThat(exactSearches.size(), equalTo(0));
    }

    public void testQueriesWithoutTheStateOfTheDataAreComputedAgain() throws IOException {
        records = List.of(record(1, null), record(2, STORED));

        GoldenRefresher.Result result = refresh(refresher(), 4, 10);

        assertThat(result, equalTo(new GoldenRefresher.Result(4, 5, 1, 1, 0)));
        assertThat(exactSearches.size(), equalTo(1));
        assertThat("nothing to compare, so no probe", probes.size(), equalTo(1));
    }

    public void testQueriesThatCannotBeLookedAtOrComputedAreLeftOutAndCounted() throws IOException {
        records = List.of(record(1, STORED), record(2, STORED), record(3, STORED), record(4, STORED));
        now = n -> n >= 3 ? CHANGED : STORED;
        failingProbes = java.util.Set.of(2L); // cannot be looked at
        failingExact = java.util.Set.of(3L); // looked at, stale, and the scan fails

        GoldenRefresher.Result result = refresh(refresher(), 4, 10);

        assertThat(result, equalTo(new GoldenRefresher.Result(4, 5, 1, 1, 2)));
        assertThat("the one that is copied and the one that is computed", bulks.get(0).numberOfActions(), equalTo(2));
        assertThat(
            "the others are not vouched for",
            bulks.get(0).requests().stream().map(DocWriteRequest::id).toList(),
            equalTo(List.of("5_1", "5_4"))
        );
    }

    public void testUnreadableRecordsAreCountedAsFailed() throws IOException {
        SearchHit broken = SearchHit.unpooled(9, "broken");
        broken.sourceRef(new BytesArray("{\"kind\":\"record\"}"));
        records = List.of(record(1, CHANGED), broken);
        now = n -> STORED;

        assertThat(refresh(refresher(), 4, 10), equalTo(new GoldenRefresher.Result(4, 5, 0, 1, 1)));
    }

    public void testNothingIsDoneWhenNoVersionIsComplete() {
        latestVersion = 0;

        assertThat(refresh(refresher(), 0, 10), equalTo(new GoldenRefresher.Result(0, 0, 0, 0, 0)));
        assertThat(probes.size(), equalTo(0));
        assertThat(manifests.size(), equalTo(0));
    }

    public void testAFailedReadIsReported() {
        GoldenReader failing = new GoldenReader((request, listener) -> listener.onFailure(new IndexNotFoundException("x")), REGISTRY);
        GoldenRefresher refresher = new GoldenRefresher(
            failing,
            new GoldenStaleness(failing, (request, listener) -> fail("nothing to look at")),
            (request, listener) -> fail("nothing to scan"),
            new GoldenWriter(
                (request, listener) -> fail("nothing to write"),
                (request, listener) -> fail("nothing to write"),
                (request, listener) -> fail("nothing to write"),
                (request, listener) -> fail("nothing to write"),
                () -> 0L
            )
        );

        // an index that does not exist is not an error: there is just nothing in it
        assertThat(refresh(refresher, 3, 10), equalTo(new GoldenRefresher.Result(3, 0, 0, 0, 0)));
        AtomicReference<Exception> failure = new AtomicReference<>();
        GoldenReader broken = new GoldenReader(
            (request, listener) -> listener.onFailure(new IllegalStateException("read failed")),
            REGISTRY
        );
        new GoldenRefresher(
            broken,
            new GoldenStaleness(broken, (request, listener) -> fail("nothing to look at")),
            (request, listener) -> fail("nothing to scan"),
            new GoldenWriter(
                (request, listener) -> fail("nothing to write"),
                (request, listener) -> fail("nothing to write"),
                (request, listener) -> fail("nothing to write"),
                (request, listener) -> fail("nothing to write"),
                () -> 0L
            )
        ).refresh(3, 10, ActionListener.wrap(result -> fail("expected a failure"), failure::set));

        assertThat(failure.get(), instanceOf(IllegalStateException.class));
    }

    public void testTheExactSearchIsTheOneOfTheQuery() throws IOException {
        records = List.of(record(1, CHANGED));
        now = n -> STORED;

        refresh(refresher(), 4, 10);

        CapturedQuery query = new CapturedQuery(new String[] { "idx1" }, "vec", new float[] { 1f }, 3, 10, null, null, List.of(), null);
        assertThat(exactSearches.get(0).source().query(), equalTo(ExactSearch.request(query).source().query()));
    }

    private static SearchHit manifest(long version) {
        SearchHit hit = SearchHit.unpooled(0, GoldenRecord.manifestId(version));
        hit.sourceRef(new BytesArray("{\"kind\":\"version\",\"dataset_version\":" + version + ",\"completed\":true}"));
        return hit;
    }

    /**
     * A record of the golden version 4, of the query whose vector starts with {@code n}, which is searched in the index {@code idx<n>}.
     */
    private static SearchHit record(long n, DataState state) throws IOException {
        CapturedQuery query = new CapturedQuery(new String[] { "idx" + n }, "vec", new float[] { n }, 3, 10, null, null, List.of(), null);
        StoredSample sample = new StoredSample(
            "sampler",
            String.valueOf(n),
            new CapturedSearch(query, List.of(), 1, 1.0),
            new TrackedQuery.Weights(1, 1.0, 1.0, 1.0, 1.0),
            1L,
            2L,
            new GroundTruth(List.of(new CapturedSearch.Hit("idx" + n, "old" + n, 1f)), state),
            null,
            null,
            null,
            null
        );
        SearchHit hit = SearchHit.unpooled((int) n, GoldenRecord.recordId(4, sample.fingerprint()));
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            hit.sourceRef(BytesReference.bytes(GoldenRecord.record(builder, 4, sample, 9L)));
        }
        return hit;
    }

    /**
     * Answers as the transport does: the response is released once the listener is done with it.
     */
    private static void respond(ActionListener<SearchResponse> listener, List<SearchHit> hits, DataState state) {
        long total = state == null ? hits.size() : state.documents();
        SearchHits searchHits = new SearchHits(hits.toArray(SearchHit[]::new), new TotalHits(total, TotalHits.Relation.EQUAL_TO), 1f);
        SearchResponse response = state == null
            ? SearchResponseUtils.successfulResponse(searchHits)
            : SearchResponseUtils.response(searchHits)
                .aggregations(InternalAggregations.from(List.of(new Sum("seq_no_sum", state.seqNoSum(), DocValueFormat.RAW, Map.of()))))
                .build();
        searchHits.decRef(); // the response holds its own reference
        try {
            listener.onResponse(response);
        } finally {
            response.decRef();
        }
    }
}
