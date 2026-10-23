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
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.DataState;
import org.elasticsearch.xpack.querysampling.groundtruth.FakeShards;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

public class GoldenUpdaterTests extends ESTestCase {

    private static final NamedXContentRegistry REGISTRY = new NamedXContentRegistry(
        new SearchModule(Settings.EMPTY, List.of()).getNamedXContents()
    );

    private static final FakeShards STORED_SHARDS = FakeShards.of(
        new FakeShards.Shard("idx", 0, 3, 6),
        new FakeShards.Shard("idx", 1, 2, 4)
    );

    private static final FakeShards CHANGED_SHARDS = FakeShards.of(
        new FakeShards.Shard("idx", 0, 3, 6),
        new FakeShards.Shard("idx", 1, 2, 9)
    );

    private static final DataState STORED = STORED_SHARDS.state();

    private static final DataState CHANGED = CHANGED_SHARDS.state();

    /** The shards of a state are read one number after the other, up to the first that none has: 0 and 1, and 2. */
    private static final int PROBES_PER_STATE = 3;

    private static FakeShards shardsOf(DataState state) {
        return state.equals(STORED) ? STORED_SHARDS : CHANGED_SHARDS;
    }

    private final List<SearchRequest> probes = new ArrayList<>();

    private final List<SearchRequest> exactSearches = new ArrayList<>();

    private final List<IndexRequest> manifests = new ArrayList<>();

    private final List<BulkRequest> bulks = new ArrayList<>();

    private List<SearchHit> goldenUpdaterStored = List.of();

    private List<SearchHit> golden = List.of();

    private long latestVersion = 0;

    private Function<Long, DataState> now = n -> STORED;

    private GoldenWriter writer() {
        return new GoldenWriter(
            (request, listener) -> { respond(listener, latestVersion == 0 ? List.of() : List.of(manifest(latestVersion))); },
            (request, listener) -> {
                manifests.add(request);
                listener.onResponse(new IndexResponse(new ShardId(GoldenIndex.NAME, "_na_", 0), request.id(), 0, 1, 1, true));
            },
            (request, listener) -> {
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
            },
            (request, listener) -> {
                listener.onResponse(
                    new UpdateResponse(new ShardId(GoldenIndex.NAME, "_na_", 0), request.id(), 0, 1, 1, DocWriteResponse.Result.UPDATED)
                );
            },
            () -> 42L
        );
    }

    private GoldenReader reader() {
        return new GoldenReader((request, listener) -> {
            if (request.source().size() == 1) {
                respond(listener, latestVersion == 0 ? List.of() : List.of(manifest(latestVersion)));
            } else {
                respond(listener, golden.stream().limit(request.source().size()).toList());
            }
        }, REGISTRY);
    }

    private StoredSamples samples() {
        return new StoredSamples((request, listener) -> { respond(listener, goldenUpdaterStored); }, REGISTRY);
    }

    private GoldenUpdater refreshing() {
        GoldenReader reader = reader();
        return new GoldenUpdater(reader, samples(), writer(), new GoldenStaleness(reader, (request, listener) -> {
            probes.add(request);
            shardsOf(now.apply(numberOf(request))).accept(request, listener);
        }, EsExecutors.DIRECT_EXECUTOR_SERVICE), (request, listener) -> {
            if (FakeShards.isProbe(request)) {
                CHANGED_SHARDS.accept(request, listener);
                return;
            }
            exactSearches.add(request);
            long n = numberOf(request);
            SearchHit hit = SearchHit.unpooled(0, "fresh" + n);
            hit.score(1f);
            hit.shard(new SearchShardTarget("node", new ShardId("idx" + n, "_na_", 0), null));
            respond(listener, List.of(hit));
        }, EsExecutors.DIRECT_EXECUTOR_SERVICE);
    }

    private GoldenUpdater.Result update(GoldenUpdater updater, int limit) {
        AtomicReference<GoldenUpdater.Result> result = new AtomicReference<>();
        updater.update(limit, ActionListener.wrap(result::set, e -> fail(e)));
        return result.get();
    }

    private static long numberOf(SearchRequest request) {
        return Long.parseLong(request.indices()[0].substring(3));
    }

    private static String fp(long n) {
        return new QueryFingerprint(n, n).hex();
    }

    private List<String> writtenIds() {
        return bulks.get(0).requests().stream().map(DocWriteRequest::id).toList();
    }

    public void testFreshQueriesAreCopiedAndStaleOnesAreComputedAgainInANewVersion() throws IOException {
        latestVersion = 4;
        golden = List.of(goldenRecord(1, 100, STORED), goldenRecord(2, 200, STORED), goldenRecord(3, 300, STORED));
        now = n -> n == 2 ? CHANGED : STORED;

        GoldenUpdater.Result result = update(refreshing(), 10);

        assertThat(result, equalTo(new GoldenUpdater.Result(4, 5, 2, 1, 0, 0, 0)));
        assertThat("only the stale one is scanned again", exactSearches.size(), equalTo(1));
        assertThat(manifests.get(0).source().utf8ToString().contains("\"source_version\":4"), equalTo(true));
        assertThat(writtenIds(), equalTo(List.of("5_" + fp(3), "5_" + fp(2), "5_" + fp(1))));
        String refreshed = ((IndexRequest) bulks.get(0).requests().get(1)).source().utf8ToString();
        assertThat(
            "the new ground truth and the state of the data it was computed on",
            refreshed.contains("\"id\":\"fresh2\""),
            equalTo(true)
        );
        assertThat(refreshed.contains("\"digest\":" + CHANGED.digest()), equalTo(true));
        String copied = ((IndexRequest) bulks.get(0).requests().get(0)).source().utf8ToString();
        assertThat("as it was", copied.contains("\"digest\":" + STORED.digest()), equalTo(true));
    }

    public void testWhenTheLimitIsLowerThanTheVersionTheNewestQueriesAreKeptWhateverTheirFingerprints() throws IOException {
        latestVersion = 4;
        golden = List.of(
            goldenRecord(1, 100, STORED),
            goldenRecord(2, 200, STORED),
            goldenRecord(3, 300, STORED),
            goldenRecord(4, 400, STORED)
        );
        goldenUpdaterStored = List.of(sample("s", 5, 500));

        GoldenUpdater.Result result = update(refreshing(), 3);

        assertThat(result, equalTo(new GoldenUpdater.Result(4, 5, 2, 0, 1, 2, 0)));
        assertThat(writtenIds(), equalTo(List.of("5_" + fp(5), "5_" + fp(4), "5_" + fp(3))));
        assertThat("the one that cannot be in it is not looked at", probes.size(), equalTo(3 * PROBES_PER_STATE));
    }

    public void testQueriesWithoutTheStateOfTheDataAreComputedAgain() throws IOException {
        latestVersion = 4;
        golden = List.of(goldenRecord(1, 100, null), goldenRecord(2, 200, STORED));

        GoldenUpdater.Result result = update(refreshing(), 10);

        assertThat(result, equalTo(new GoldenUpdater.Result(4, 5, 1, 1, 0, 0, 0)));
        assertThat(exactSearches.size(), equalTo(1));
        assertThat("nothing to compare, so no probe for it", probes.size(), equalTo(PROBES_PER_STATE));
    }

    private static SearchHit manifest(long version) {
        SearchHit hit = SearchHit.unpooled(0, GoldenRecord.manifestId(version));
        hit.sourceRef(new BytesArray("{\"kind\":\"version\",\"dataset_version\":" + version + ",\"completed\":true}"));
        return hit;
    }

    private static StoredSample storedSample(long n, long pickedAt, String groundTruthId, DataState state) {
        CapturedQuery query = new CapturedQuery(new String[] { "idx" + n }, "vec", new float[] { n }, 3, 10, null, null, List.of(), null);
        return new StoredSample(
            "sampler",
            fp(n),
            new CapturedSearch(query, List.of(), 1, 1.0),
            new TrackedQuery.Weights(1, 1.0, 1.0, 1.0),
            pickedAt,
            pickedAt,
            new GroundTruth(List.of(new CapturedSearch.Hit("idx" + n, groundTruthId, 1f)), state),
            null,
            null,
            null,
            null
        );
    }

    private static SearchHit goldenRecord(long n, long pickedAt, DataState state) throws IOException {
        SearchHit hit = SearchHit.unpooled((int) n, GoldenRecord.recordId(2, fp(n)));
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            hit.sourceRef(BytesReference.bytes(GoldenRecord.record(builder, 2, storedSample(n, pickedAt, "golden" + n, state), 9L)));
        }
        return hit;
    }

    private static SearchHit sample(String samplerId, long n, long pickedAt) throws IOException {
        QueryFingerprint fingerprint = new QueryFingerprint(n, n);
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", new float[] { n }, 3, 10, null, null, List.of(), null);
        SampledQuery sampled = new SampledQuery(
            fingerprint,
            new CapturedSearch(query, List.of(), 1, 1.0),
            new MultiplicityTracker(10).record(fingerprint)
        );
        // a picked query gets its ground truth as an update, so the stored document is the two put together
        XContentBuilder picked = SampleRecord.document(JsonXContent.contentBuilder(), samplerId, sampled, pickedAt);
        Map<String, Object> source = new HashMap<>(toMap(picked));
        GroundTruth groundTruth = new GroundTruth(List.of(new CapturedSearch.Hit("idx", "goldenUpdaterStored" + n, 1f)), null);
        source.putAll(toMap(SampleRecord.groundTruthUpdate(JsonXContent.contentBuilder(), groundTruth, pickedAt)));
        SearchHit hit = SearchHit.unpooled((int) n, SampleRecord.documentId(samplerId, fingerprint));
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            hit.sourceRef(BytesReference.bytes(builder.map(source)));
        }
        return hit;
    }

    private static Map<String, Object> toMap(XContentBuilder builder) {
        return XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
    }

    private static void respond(ActionListener<SearchResponse> listener, List<SearchHit> hits) {
        SearchHits searchHits = new SearchHits(hits.toArray(SearchHit[]::new), new TotalHits(hits.size(), TotalHits.Relation.EQUAL_TO), 1f);
        SearchResponse response = SearchResponseUtils.successfulResponse(searchHits);
        searchHits.decRef();
        try {
            listener.onResponse(response);
        } finally {
            response.decRef();
        }
    }
}
