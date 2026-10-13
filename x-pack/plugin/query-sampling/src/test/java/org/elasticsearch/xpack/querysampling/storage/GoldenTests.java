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
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SystemIndices;
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
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.Hardness;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.Selectivity;
import org.elasticsearch.xpack.querysampling.dedup.Stratum;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.DataState;
import org.elasticsearch.xpack.querysampling.groundtruth.FakeShards;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class GoldenTests extends ESTestCase {

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

    /**
     * The shards of the state are read one number after the other, up to the first that none has: 0 and 1, and 2.
     */
    private static final int PROBES_PER_STATE = 3;

    private static FakeShards shardsOf(DataState state) {
        return state.equals(STORED) ? STORED_SHARDS : CHANGED_SHARDS;
    }

    private final List<SearchRequest> sampleSearches = new ArrayList<>();

    private final List<SearchRequest> goldenSearches = new ArrayList<>();

    private final List<SearchRequest> probes = new ArrayList<>();

    private final List<SearchRequest> exactSearches = new ArrayList<>();

    private final List<IndexRequest> manifests = new ArrayList<>();

    private final List<BulkRequest> bulks = new ArrayList<>();

    private final List<UpdateRequest> completions = new ArrayList<>();

    private List<SearchHit> goldenUpdaterStored = List.of();

    private List<SearchHit> golden = List.of();

    private long latestVersion = 0;

    private boolean failSampleRead;

    private boolean noGoldenIndex;

    private Function<Long, DataState> now = n -> STORED;

    private Set<Long> failingProbes = Set.of();

    private Set<Long> failingExact = Set.of();

    private GoldenWriter writer() {
        return new GoldenWriter((request, listener) -> {
            if (noGoldenIndex) {
                listener.onFailure(new IndexNotFoundException(GoldenIndex.NAME));
            } else {
                respond(listener, latestVersion == 0 ? List.of() : List.of(manifest(latestVersion)));
            }
        }, (request, listener) -> {
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
            completions.add(request);
            listener.onResponse(
                new UpdateResponse(new ShardId(GoldenIndex.NAME, "_na_", 0), request.id(), 0, 1, 1, DocWriteResponse.Result.UPDATED)
            );
        }, () -> 42L);
    }

    private GoldenReader reader() {
        return new GoldenReader((request, listener) -> {
            goldenSearches.add(request);
            if (noGoldenIndex) {
                listener.onFailure(new IndexNotFoundException(GoldenIndex.NAME));
            } else if (request.source().size() == 1) {
                respond(listener, latestVersion == 0 ? List.of() : List.of(manifest(latestVersion)));
            } else {
                respond(listener, golden.stream().limit(request.source().size()).toList());
            }
        }, REGISTRY);
    }

    private StoredSamples samples() {
        return new StoredSamples((request, listener) -> {
            sampleSearches.add(request);
            if (failSampleRead) {
                listener.onFailure(new IllegalStateException("read failed"));
            } else {
                respond(listener, goldenUpdaterStored);
            }
        }, REGISTRY);
    }

    private GoldenUpdater adding() {
        return new GoldenUpdater(reader(), samples(), writer(), null, null);
    }

    private GoldenUpdater refreshing() {
        GoldenReader reader = reader();
        return new GoldenUpdater(reader, samples(), writer(), new GoldenStaleness(reader, (request, listener) -> {
            probes.add(request);
            long n = numberOf(request);
            if (failingProbes.contains(n)) {
                listener.onFailure(new IllegalStateException("no"));
            } else {
                shardsOf(now.apply(n)).accept(request, listener);
            }
        }), (request, listener) -> {
            if (FakeShards.isProbe(request)) {
                CHANGED_SHARDS.accept(request, listener);
                return;
            }
            exactSearches.add(request);
            long n = numberOf(request);
            if (failingExact.contains(n)) {
                listener.onFailure(new IllegalStateException("no"));
                return;
            }
            SearchHit hit = SearchHit.unpooled(0, "fresh" + n);
            hit.score(1f);
            hit.shard(new SearchShardTarget("node", new ShardId("idx" + n, "_na_", 0), null));
            respond(listener, List.of(hit));
        });
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

    public void testWithoutAGoldenDatasetTheStoredQueriesMakeTheFirstVersion() throws IOException {
        goldenUpdaterStored = List.of(sample("s", 3, 300), sample("s", 2, 200), sample("s", 1, 100));

        GoldenUpdater.Result result = update(adding(), 10);

        assertThat(result, equalTo(new GoldenUpdater.Result(0, 1, 0, 0, 3, 0, 0)));
        assertThat(manifests.get(0).id(), equalTo("version_1"));
        assertThat("it is not made from another", manifests.get(0).source().utf8ToString().contains("source_version"), equalTo(false));
        assertThat(bulks.get(0).numberOfActions(), equalTo(3));
        assertThat(
            "the next update has to find the version",
            completions.get(0).getRefreshPolicy(),
            equalTo(WriteRequest.RefreshPolicy.IMMEDIATE)
        );
    }

    public void testOnlyQueriesThatAreNotInTheLatestVersionAreAddedAndTheOnesThereAreKept() throws IOException {
        latestVersion = 2;
        golden = List.of(goldenRecord(1, 100, STORED), goldenRecord(2, 200, STORED));
        goldenUpdaterStored = List.of(sample("s", 4, 400), sample("s", 3, 300), sample("s", 2, 200), sample("s", 1, 100));

        GoldenUpdater.Result result = update(adding(), 10);

        assertThat(result, equalTo(new GoldenUpdater.Result(2, 3, 2, 0, 2, 0, 0)));
        assertThat(manifests.get(0).source().utf8ToString().contains("\"source_version\":2"), equalTo(true));
        assertThat("the newest first", writtenIds(), equalTo(List.of("3_" + fp(4), "3_" + fp(3), "3_" + fp(2), "3_" + fp(1))));
        assertThat("the ground truth is not looked at", probes.size(), equalTo(0));
    }

    public void testWhatWasInTheVersionKeepsItsGroundTruth() throws IOException {
        latestVersion = 2;
        golden = List.of(goldenRecord(1, 100, STORED));
        goldenUpdaterStored = List.of(sample("s", 2, 200), sample("s", 1, 100));

        update(adding(), 10);

        String copy = ((IndexRequest) bulks.get(0).requests().get(1)).source().utf8ToString();
        assertThat(
            "not the one of the goldenUpdaterStored query, which would be of other data",
            copy.contains("\"id\":\"golden1\""),
            equalTo(true)
        );
    }

    public void testWhenThereAreMoreThanTheLimitTheOnesPickedLongestAgoAreLeftOut() throws IOException {
        latestVersion = 2;
        golden = List.of(goldenRecord(1, 100, STORED), goldenRecord(2, 200, STORED));
        goldenUpdaterStored = List.of(sample("s", 4, 400), sample("s", 3, 300));

        GoldenUpdater.Result result = update(adding(), 3);

        assertThat(result, equalTo(new GoldenUpdater.Result(2, 3, 1, 0, 2, 1, 0)));
        assertThat(writtenIds(), equalTo(List.of("3_" + fp(4), "3_" + fp(3), "3_" + fp(2))));
    }

    public void testNoVersionIsMadeWhenNothingIsNew() throws IOException {
        latestVersion = 2;
        golden = List.of(goldenRecord(1, 100, STORED), goldenRecord(2, 200, STORED));
        goldenUpdaterStored = List.of(sample("s", 2, 200), sample("s", 1, 100));

        assertThat(update(adding(), 10), equalTo(new GoldenUpdater.Result(2, 0, 2, 0, 0, 0, 0)));
        assertThat(manifests.size(), equalTo(0));
        assertThat(bulks.size(), equalTo(0));
    }

    public void testAQueryIsInTheVersionOnceWhicheverSamplersPickedIt() throws IOException {
        goldenUpdaterStored = List.of(sample("second", 1, 300), sample("first", 1, 100), sample("first", 2, 50));

        GoldenUpdater.Result result = update(adding(), 10);

        assertThat(result.added(), equalTo(2));
        assertThat(
            ((IndexRequest) bulks.get(0).requests().get(0)).source().utf8ToString().contains("\"sampler_id\":\"second\""),
            equalTo(true)
        );
    }

    public void testOnlyQueriesWithGroundTruthAndNotEventsAreRead() {
        update(adding(), 10);

        assertThat(
            sampleSearches.get(0).source().query(),
            equalTo(
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("has_ground_truth", true))
                    .mustNot(QueryBuilders.existsQuery("event_id"))
            )
        );
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

    public void testWhatIsComputedAgainAndWhatIsAddedAreInTheSameVersion() throws IOException {
        latestVersion = 4;
        golden = List.of(goldenRecord(1, 100, STORED));
        now = n -> CHANGED;
        goldenUpdaterStored = List.of(sample("s", 2, 200));

        assertThat(update(refreshing(), 10), equalTo(new GoldenUpdater.Result(4, 5, 0, 1, 1, 0, 0)));
        assertThat(bulks.get(0).numberOfActions(), equalTo(2));
    }

    public void testNoVersionIsMadeWhenEverythingIsFreshAndNothingIsNew() throws IOException {
        latestVersion = 4;
        golden = List.of(goldenRecord(1, 100, STORED), goldenRecord(2, 200, STORED));

        GoldenUpdater.Result result = update(refreshing(), 10);

        assertThat(result, equalTo(new GoldenUpdater.Result(4, 0, 2, 0, 0, 0, 0)));
        assertThat("a copy of the version there is would be of no use", manifests.size(), equalTo(0));
        assertThat(exactSearches.size(), equalTo(0));
    }

    public void testQueriesWithoutTheStateOfTheDataAreComputedAgain() throws IOException {
        latestVersion = 4;
        golden = List.of(goldenRecord(1, 100, null), goldenRecord(2, 200, STORED));

        GoldenUpdater.Result result = update(refreshing(), 10);

        assertThat(result, equalTo(new GoldenUpdater.Result(4, 5, 1, 1, 0, 0, 0)));
        assertThat(exactSearches.size(), equalTo(1));
        assertThat("nothing to compare, so no probe for it", probes.size(), equalTo(PROBES_PER_STATE));
    }

    public void testQueriesThatCannotBeLookedAtOrComputedAreLeftOutAndCounted() throws IOException {
        latestVersion = 4;
        golden = List.of(
            goldenRecord(1, 100, STORED),
            goldenRecord(2, 200, STORED),
            goldenRecord(3, 300, STORED),
            goldenRecord(4, 400, STORED)
        );
        now = n -> n >= 3 ? CHANGED : STORED;
        failingProbes = Set.of(2L);
        failingExact = Set.of(3L);

        GoldenUpdater.Result result = update(refreshing(), 10);

        assertThat(result, equalTo(new GoldenUpdater.Result(4, 5, 1, 1, 0, 0, 2)));
        assertThat("the others are not vouched for", writtenIds(), equalTo(List.of("5_" + fp(4), "5_" + fp(1))));
    }

    public void testNoVersionIsMadeWhenAllThatChangedCouldNotBeComputedAndNothingIsNew() throws IOException {
        latestVersion = 4;
        golden = List.of(goldenRecord(1, 100, STORED));
        now = n -> CHANGED;
        failingExact = Set.of(1L);

        assertThat(update(refreshing(), 10), equalTo(new GoldenUpdater.Result(4, 0, 0, 0, 0, 0, 1)));
        assertThat(manifests.size(), equalTo(0));
    }

    public void testWhatIsComputedAgainIsSubjectToTheLimitToo() throws IOException {
        latestVersion = 4;
        golden = List.of(goldenRecord(1, 100, STORED), goldenRecord(2, 200, STORED));
        now = n -> CHANGED;
        goldenUpdaterStored = List.of(sample("s", 3, 300));

        GoldenUpdater.Result result = update(refreshing(), 2);

        assertThat("the oldest is left out, though it was computed again", result, equalTo(new GoldenUpdater.Result(4, 5, 0, 1, 1, 1, 0)));
        assertThat(writtenIds(), equalTo(List.of("5_" + fp(3), "5_" + fp(2))));
    }

    private static SearchHit manifest(long version) {
        SearchHit hit = SearchHit.unpooled(0, GoldenRecord.manifestId(version));
        hit.sourceRef(new BytesArray("{\"kind\":\"version\",\"dataset_version\":" + version + ",\"completed\":true}"));
        return hit;
    }

    private static StoredSample goldenUpdaterStored(long n, long pickedAt, String groundTruthId, DataState state) {
        CapturedQuery query = new CapturedQuery(new String[] { "idx" + n }, "vec", new float[] { n }, 3, 10, null, null, List.of(), null);
        return new StoredSample(
            "sampler",
            fp(n),
            new CapturedSearch(query, List.of(), 1, 1.0),
            new TrackedQuery.Weights(1, 1.0, 1.0, 1.0, 1.0),
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
            hit.sourceRef(BytesReference.bytes(GoldenRecord.record(builder, 2, goldenUpdaterStored(n, pickedAt, "golden" + n, state), 9L)));
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
        sampled.attach(GroundTruth.KEY, new GroundTruth(List.of(new CapturedSearch.Hit("idx", "goldenUpdaterStored" + n, 1f))));
        SearchHit hit = SearchHit.unpooled((int) n, SampleRecord.documentId(samplerId, fingerprint));
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            hit.sourceRef(BytesReference.bytes(SampleRecord.document(builder, samplerId, sampled, pickedAt)));
        }
        return hit;
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

    private List<SearchHit> records = List.of();

    private Long latestCompleted = 3L;

    private Function<Float, DataState> goldenStalenessNow = x -> STORED;

    private GoldenStaleness goldenStalenessStaleness(BiConsumer<SearchRequest, ActionListener<SearchResponse>> probe) {
        GoldenReader reader = new GoldenReader((request, listener) -> {
            goldenSearches.add(request);
            if (latestCompleted == null) {
                listener.onFailure(new IndexNotFoundException(GoldenIndex.NAME));
            } else if (request.source().size() == 1) {
                respond(listener, latestCompleted == 0 ? List.of() : List.of(manifest(latestCompleted)));
            } else {
                respond(listener, records);
            }
        }, REGISTRY);
        return new GoldenStaleness(reader, (request, listener) -> {
            probes.add(request);
            probe.accept(request, listener);
        });
    }

    private GoldenStaleness goldenStalenessStaleness() {
        return goldenStalenessStaleness(
            (request, listener) -> { shardsOf(goldenStalenessNow.apply(firstOf(request))).accept(request, listener); }
        );
    }

    private static float firstOf(SearchRequest request) {
        return Float.parseFloat(request.indices()[0].substring(3));
    }

    private GoldenStaleness.Result check(GoldenStaleness goldenStalenessStaleness, long version, int max) {
        AtomicReference<GoldenStaleness.Result> result = new AtomicReference<>();
        goldenStalenessStaleness.check(version, max, ActionListener.wrap(result::set, e -> fail(e)));
        return result.get();
    }

    public void testQueriesWhoseDataIsAsItWasAreFreshAndThoseWhoseDataChangedAreStale() throws IOException {
        records = List.of(record(1, STORED), record(2, STORED), record(3, CHANGED));
        goldenStalenessNow = x -> STORED;

        GoldenStaleness.Result result = check(goldenStalenessStaleness(), 3, 10);

        assertThat(result, equalTo(new GoldenStaleness.Result(3, 3, 2, 1, 0, 0)));
    }

    public void testGroundTruthWithoutTheStateOfTheDataCannotBeTold() throws IOException {
        records = List.of(record(1, null), record(2, STORED));

        GoldenStaleness.Result result = check(goldenStalenessStaleness(), 3, 10);

        assertThat(result, equalTo(new GoldenStaleness.Result(3, 2, 1, 0, 1, 0)));
        assertThat("there is nothing to compare, so no search is made for it", probes.size(), equalTo(PROBES_PER_STATE));
    }

    public void testManyQueriesWithoutTheStateOfTheDataDoNotExhaustTheStack() {
        List<StoredSample> many = new ArrayList<>();
        for (int i = 0; i < 100_000; i++) {
            many.add(goldenUpdaterStored(1, 100, "d", null));
        }
        AtomicReference<List<GoldenStaleness.Status>> statuses = new AtomicReference<>();

        goldenStalenessStaleness().classify(many, ActionListener.wrap(statuses::set, e -> fail(e)));

        assertThat(statuses.get().size(), equalTo(many.size()));
        assertThat(Set.copyOf(statuses.get()), equalTo(Set.of(GoldenStaleness.Status.UNKNOWN)));
    }

    public void testWithoutAVersionTheLatestOneThatIsCompleteIsChecked() throws IOException {
        records = List.of(record(1, STORED));
        latestCompleted = 7L;

        GoldenStaleness.Result result = check(goldenStalenessStaleness(), 0, 10);

        assertThat(result.version(), equalTo(7L));
        assertThat(
            "the version that was found is the one read",
            goldenSearches.get(1).source().query().toString().contains("\"value\" : 7"),
            equalTo(true)
        );
        assertThat(goldenSearches.get(1).source().size(), equalTo(10));
    }

    public void testAFailedProbeIsCountedAsFailedAndDoesNotStopTheOthers() throws IOException {
        records = List.of(record(1, STORED), record(2, STORED), record(3, STORED));
        GoldenStaleness goldenStalenessStaleness = goldenStalenessStaleness((request, listener) -> {
            if (firstOf(request) == 2f) {
                listener.onFailure(new IllegalStateException("no"));
            } else {
                STORED_SHARDS.accept(request, listener);
            }
        });

        assertThat(check(goldenStalenessStaleness, 3, 10), equalTo(new GoldenStaleness.Result(3, 3, 2, 0, 0, 1)));
    }

    public void testAProbeThatThrowsAndAResponseWithoutAStateAreFailures() throws IOException {
        records = List.of(record(1, STORED), record(2, STORED));
        GoldenStaleness goldenStalenessStaleness = goldenStalenessStaleness((request, listener) -> {
            if (firstOf(request) == 1f) {
                throw new IllegalStateException("no");
            }
            respond(listener, List.of());
        });

        assertThat(check(goldenStalenessStaleness, 3, 10), equalTo(new GoldenStaleness.Result(3, 2, 0, 0, 0, 2)));
    }

    public void testUnreadableRecordsAreCountedAsFailed() throws IOException {
        SearchHit broken = SearchHit.unpooled(9, "broken");
        broken.sourceRef(new BytesArray("{\"kind\":\"record\"}"));
        records = List.of(record(1, STORED), broken);

        assertThat(check(goldenStalenessStaleness(), 3, 10), equalTo(new GoldenStaleness.Result(3, 2, 1, 0, 0, 1)));
    }

    private static SearchHit record(long n, DataState state) throws IOException {
        CapturedQuery query = new CapturedQuery(new String[] { "idx" + n }, "vec", new float[] { n }, 3, 10, null, null, List.of(), null);
        StoredSample sample = new StoredSample(
            "sampler",
            String.valueOf(n),
            new CapturedSearch(query, List.of(), 1, 1.0),
            new TrackedQuery.Weights(1, 1.0, 1.0, 1.0, 1.0),
            1L,
            2L,
            new GroundTruth(List.of(new CapturedSearch.Hit("idx" + n, "d" + n, 1f)), state),
            null,
            null,
            null,
            null
        );
        SearchHit hit = SearchHit.unpooled((int) n, GoldenRecord.recordId(3, sample.fingerprint()));
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            hit.sourceRef(BytesReference.bytes(GoldenRecord.record(builder, 3, sample, 9L)));
        }
        return hit;
    }

    private static StoredSample goldenRecordSample(Stratum stratum, Hardness hardness, Selectivity selectivity) {
        CapturedQuery query = new CapturedQuery(
            new String[] { "a", "b" },
            "vec",
            new float[] { 1f, 2f },
            10,
            100,
            0.5f,
            3f,
            List.of(QueryBuilders.termQuery("category", 3)),
            "q7"
        );
        TrackedQuery.Weights weights = new TrackedQuery.Weights(3, 30.0, 0.5, 0.25, 0.1);
        GroundTruth groundTruth = new GroundTruth(List.of(new CapturedSearch.Hit("a", "d1", 1f)), STORED);
        return new StoredSample(
            "s1",
            "0000000000000001ffffffffffffffff",
            new CapturedSearch(query, List.of(new CapturedSearch.Hit("a", "d1", 0.9f)), 12, 0.1),
            weights,
            1000L,
            2000L,
            groundTruth,
            stratum,
            hardness,
            null,
            selectivity
        );
    }

    @SuppressWarnings("unchecked")
    public void testRecordIsACopyOfTheStoredSampleWithItsGroundTruthAndTheVersion() throws IOException {
        StoredSample goldenRecordSample = goldenRecordSample(new Stratum("vec/2", 3), Hardness.HARD, Selectivity.LOW);

        Map<String, Object> record = toMap(GoldenRecord.record(JsonXContent.contentBuilder(), 4, goldenRecordSample, 9000L));

        assertThat(record.get("kind"), equalTo("record"));
        assertThat(record.get("dataset_version"), equalTo(4));
        assertThat(record.get("promoted_at"), equalTo(9000));
        assertThat("when it was picked, and not when it was written last", record.get("source_picked_at"), equalTo(1000));
        assertThat(record.get("sampler_id"), equalTo("s1"));
        assertThat(record.get("fingerprint"), equalTo(goldenRecordSample.fingerprint()));
        assertThat(record.get("indices"), equalTo(List.of("a", "b")));
        assertThat(record.get("multiplicity"), equalTo(3));
        assertThat(record.get("weighted_multiplicity"), equalTo(30.0));
        assertThat(record.get("inclusion_probability"), equalTo(0.5));
        assertThat(record.get("capture_rate"), equalTo(0.1));
        assertThat(record.get("spatial_space"), equalTo("vec/2"));
        assertThat(record.get("spatial_cluster"), equalTo(3));
        assertThat(record.get("hardness"), equalTo("hard"));
        assertThat(record.get("selectivity"), equalTo("low"));
        assertThat(((Map<String, Object>) record.get("query")).get("query_vector"), equalTo(List.of(1.0, 2.0)));
        assertThat(((List<?>) ((Map<String, Object>) record.get("live_hits")).get("hits")).size(), equalTo(1));
        Map<String, Object> groundTruth = (Map<String, Object>) record.get("ground_truth");
        assertThat(((List<?>) groundTruth.get("neighbors")).size(), equalTo(1));
        assertThat(groundTruth.get("data_state"), equalTo(Map.of("documents", 5, "digest", STORED.digest())));
        assertThat("it is not a goldenRecordSample, and has nothing to refresh", record, not(hasKey("has_ground_truth")));
        assertThat(record, not(hasKey("updated_at")));
    }

    public void testRecordsAreReadBackAsTheSamplesTheyWereCopiedFrom() throws IOException {
        StoredSample goldenRecordSample = goldenRecordSample(new Stratum("vec/2", 3), Hardness.HARD, Selectivity.LOW);

        Map<String, Object> record = toMap(GoldenRecord.record(JsonXContent.contentBuilder(), 4, goldenRecordSample, 9000L));
        StoredSample read = GoldenRecord.parse(
            record,
            new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents())
        );

        assertThat(read.samplerId(), equalTo(goldenRecordSample.samplerId()));
        assertThat(read.fingerprint(), equalTo(goldenRecordSample.fingerprint()));
        assertThat(read.weights(), equalTo(goldenRecordSample.weights()));
        assertThat(read.groundTruth(), equalTo(goldenRecordSample.groundTruth()));
        assertThat(read.stratum(), equalTo(goldenRecordSample.stratum()));
        assertThat(read.hardness(), equalTo(goldenRecordSample.hardness()));
        assertThat(read.selectivity(), equalTo(goldenRecordSample.selectivity()));
        assertThat(read.search().hits(), equalTo(goldenRecordSample.search().hits()));
        assertThat("when it was picked", read.pickedAt(), equalTo(1000L));
        assertThat("when it was promoted", read.updatedAt(), equalTo(9000L));
        assertThat(read.search().query().field(), equalTo("vec"));
        assertThat(read.search().query().filters(), equalTo(goldenRecordSample.search().query().filters()));
    }

    @SuppressWarnings("unchecked")
    public void testEveryFieldOfADocumentIsInTheMappings() throws IOException {
        Set<String> written = new HashSet<>();
        written.addAll(
            toMap(
                GoldenRecord.record(
                    JsonXContent.contentBuilder(),
                    1,
                    goldenRecordSample(new Stratum("vec/2", 0), Hardness.EASY, Selectivity.HIGH),
                    1L
                )
            ).keySet()
        );
        written.addAll(toMap(GoldenRecord.manifest(JsonXContent.contentBuilder(), 1, 1L, 0, false, 3L)).keySet());
        Map<String, Object> root = XContentHelper.convertToMap(BytesReference.bytes(GoldenIndex.mappings()), false, XContentType.JSON).v2();
        Map<String, Object> mapping = (Map<String, Object>) root.get("_doc");

        assertThat(((Map<String, Object>) mapping.get("properties")).keySet(), equalTo(written));
    }

    private static Map<String, Object> toMap(XContentBuilder builder) {
        return XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
    }

    public void testTheTwoIndicesDoNotOverlap() {
        new SystemIndices(
            List.of(
                new SystemIndices.Feature(
                    "query_sampling",
                    "the indices of the query sampling",
                    List.of(QuerySamplingIndex.descriptor(), GoldenIndex.descriptor())
                )
            )
        );

        assertFalse(QuerySamplingIndex.descriptor().matchesIndexPattern(GoldenIndex.NAME));
        assertFalse(GoldenIndex.descriptor().matchesIndexPattern(QuerySamplingIndex.NAME));
    }

    private final List<Integer> limits = new ArrayList<>();

    private final List<ActionListener<GoldenUpdater.Result>> held = new ArrayList<>();

    private boolean master = true;

    private boolean completeAtOnce = true;

    private GoldenUpdater.Result outcome = new GoldenUpdater.Result(1, 2, 0, 0, 3, 0, 0);

    private Exception failure;

    private GoldenPromotionJob job() {
        GoldenPromotionJob job = new GoldenPromotionJob((limit, listener) -> {
            limits.add(limit);
            if (failure != null) {
                listener.onFailure(failure);
            } else if (completeAtOnce) {
                listener.onResponse(outcome);
            } else {
                held.add(listener);
            }
        }, () -> master);
        job.watch(
            new ClusterSettings(
                Settings.builder().put(QuerySamplingSettings.GOLDEN_MAX_QUERIES.getKey(), 7).build(),
                Set.of(QuerySamplingSettings.GOLDEN_MAX_QUERIES)
            )
        );
        return job;
    }

    public void testOnlyTheElectedMasterGrowsTheDataset() {
        master = false;
        GoldenPromotionJob job = job();

        job.run();

        assertThat(limits.size(), equalTo(0));
        assertThat(job.promotions(), equalTo(0L));
    }

    public void testTheDatasetIsGrownWithTheLimitOfTheSetting() {
        GoldenPromotionJob job = job();

        job.run();

        assertThat(limits, equalTo(List.of(7)));
        assertThat(job.promotions(), equalTo(1L));
        assertThat(job.failed(), equalTo(0L));
    }

    public void testARoundThatFindsTheOneBeforeStillGoingDoesNothing() {
        completeAtOnce = false;
        GoldenPromotionJob job = job();

        job.run();
        job.run();

        assertThat(limits.size(), equalTo(1));
        held.get(0).onResponse(outcome);
        job.run();
        assertThat("once it is over the next one runs", limits.size(), equalTo(2));
    }

    public void testAFailedRoundIsCountedAndDoesNotStopTheNext() {
        failure = new IllegalStateException("no");
        GoldenPromotionJob job = job();

        job.run();
        failure = null;
        job.run();

        assertThat(job.failed(), equalTo(1L));
        assertThat(job.promotions(), equalTo(1L));
    }
}
