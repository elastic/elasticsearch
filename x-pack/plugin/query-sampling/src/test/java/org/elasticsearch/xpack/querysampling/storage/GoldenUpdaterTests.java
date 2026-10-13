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
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
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
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.DataState;
import org.elasticsearch.xpack.querysampling.groundtruth.ExactSearch;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class GoldenUpdaterTests extends ESTestCase {

    private static final NamedXContentRegistry REGISTRY = new NamedXContentRegistry(
        new SearchModule(Settings.EMPTY, List.of()).getNamedXContents()
    );
    private static final DataState STORED = new DataState(5, 10.0);
    private static final DataState CHANGED = new DataState(6, 15.0);

    private final List<SearchRequest> sampleSearches = new ArrayList<>();
    private final List<SearchRequest> goldenSearches = new ArrayList<>();
    private final List<SearchRequest> probes = new ArrayList<>();
    private final List<SearchRequest> exactSearches = new ArrayList<>();
    private final List<IndexRequest> manifests = new ArrayList<>();
    private final List<BulkRequest> bulks = new ArrayList<>();

    private List<SearchHit> stored = List.of(); // the sampled queries that have ground truth, newest first
    private List<SearchHit> golden = List.of(); // the records of the latest version
    private long latestVersion = 0;
    private boolean failSampleRead;
    private boolean noGoldenIndex; // nothing was ever promoted, so there is no index
    private Function<Long, DataState> now = n -> STORED; // the state of the data of the query of the record n, as the probe tells it
    private Set<Long> failingProbes = Set.of();
    private Set<Long> failingExact = Set.of();

    private GoldenWriter writer() {
        return new GoldenWriter((request, listener) -> {
            if (noGoldenIndex) {
                listener.onFailure(new IndexNotFoundException(GoldenIndex.NAME));
            } else {
                respond(listener, latestVersion == 0 ? List.of() : List.of(manifest(latestVersion)), null);
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
        },
            (request, listener) -> listener.onResponse(
                new UpdateResponse(new ShardId(GoldenIndex.NAME, "_na_", 0), request.id(), 0, 1, 1, DocWriteResponse.Result.UPDATED)
            ),
            () -> 42L
        );
    }

    private GoldenReader reader() {
        return new GoldenReader((request, listener) -> {
            goldenSearches.add(request);
            if (noGoldenIndex) {
                listener.onFailure(new IndexNotFoundException(GoldenIndex.NAME));
            } else if (request.source().size() == 1) {
                respond(listener, latestVersion == 0 ? List.of() : List.of(manifest(latestVersion)), null);
            } else {
                respond(listener, golden, null);
            }
        }, REGISTRY);
    }

    private StoredSamples samples() {
        return new StoredSamples((request, listener) -> {
            sampleSearches.add(request);
            if (failSampleRead) {
                listener.onFailure(new IllegalStateException("read failed"));
            } else {
                respond(listener, stored, null);
            }
        }, REGISTRY);
    }

    /**
     * An updater that only adds, as the one of the schedule.
     */
    private GoldenUpdater adding() {
        return new GoldenUpdater(reader(), samples(), writer(), null, null);
    }

    /**
     * An updater that also looks at the data of the queries of the version and computes their ground truth again.
     */
    private GoldenUpdater refreshing() {
        GoldenReader reader = reader();
        return new GoldenUpdater(reader, samples(), writer(), new GoldenStaleness(reader, (request, listener) -> {
            probes.add(request);
            long n = numberOf(request);
            if (failingProbes.contains(n)) {
                listener.onFailure(new IllegalStateException("no"));
            } else {
                respond(listener, List.of(), now.apply(n));
            }
        }), (request, listener) -> {
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

    // what is added

    public void testWithoutAGoldenDatasetTheStoredQueriesMakeTheFirstVersion() throws IOException {
        stored = List.of(sample("s", 3, 300), sample("s", 2, 200), sample("s", 1, 100));

        GoldenUpdater.Result result = update(adding(), 10);

        assertThat(result, equalTo(new GoldenUpdater.Result(0, 1, 0, 0, 3, 0, 0)));
        assertThat(manifests.get(0).id(), equalTo("version_1"));
        assertThat("it is not made from another", manifests.get(0).source().utf8ToString().contains("source_version"), equalTo(false));
        assertThat(bulks.get(0).numberOfActions(), equalTo(3));
    }

    public void testOnlyQueriesThatAreNotInTheLatestVersionAreAddedAndTheOnesThereAreKept() throws IOException {
        latestVersion = 2;
        golden = List.of(goldenRecord(1, 100, STORED), goldenRecord(2, 200, STORED));
        stored = List.of(sample("s", 4, 400), sample("s", 3, 300), sample("s", 2, 200), sample("s", 1, 100));

        GoldenUpdater.Result result = update(adding(), 10);

        assertThat(result, equalTo(new GoldenUpdater.Result(2, 3, 2, 0, 2, 0, 0)));
        assertThat(manifests.get(0).source().utf8ToString().contains("\"source_version\":2"), equalTo(true));
        assertThat("the newest first", writtenIds(), equalTo(List.of("3_" + fp(4), "3_" + fp(3), "3_" + fp(2), "3_" + fp(1))));
        assertThat("the ground truth is not looked at", probes.size(), equalTo(0));
    }

    public void testWhatWasInTheVersionKeepsItsGroundTruth() throws IOException {
        latestVersion = 2;
        golden = List.of(goldenRecord(1, 100, STORED));
        stored = List.of(sample("s", 2, 200), sample("s", 1, 100));

        update(adding(), 10);

        String copy = ((IndexRequest) bulks.get(0).requests().get(1)).source().utf8ToString();
        assertThat("not the one of the stored query, which would be of other data", copy.contains("\"id\":\"golden1\""), equalTo(true));
    }

    public void testWhenThereAreMoreThanTheLimitTheOnesPickedLongestAgoAreLeftOut() throws IOException {
        latestVersion = 2;
        golden = List.of(goldenRecord(1, 100, STORED), goldenRecord(2, 200, STORED));
        stored = List.of(sample("s", 4, 400), sample("s", 3, 300));

        GoldenUpdater.Result result = update(adding(), 3);

        assertThat(result, equalTo(new GoldenUpdater.Result(2, 3, 1, 0, 2, 1, 0)));
        assertThat(writtenIds(), equalTo(List.of("3_" + fp(4), "3_" + fp(3), "3_" + fp(2))));
    }

    public void testNoVersionIsMadeWhenNothingIsNew() throws IOException {
        latestVersion = 2;
        golden = List.of(goldenRecord(1, 100, STORED), goldenRecord(2, 200, STORED));
        stored = List.of(sample("s", 2, 200), sample("s", 1, 100));

        assertThat(update(adding(), 10), equalTo(new GoldenUpdater.Result(2, 0, 2, 0, 0, 0, 0)));
        assertThat(manifests.size(), equalTo(0));
        assertThat(bulks.size(), equalTo(0));
    }

    public void testNoVersionIsMadeWhenWhatIsNewIsOlderThanAllThatIsThereAndTheVersionIsFull() throws IOException {
        latestVersion = 2;
        golden = List.of(goldenRecord(2, 200, STORED), goldenRecord(3, 300, STORED));
        stored = List.of(sample("s", 1, 100));

        assertThat("it would not change", update(adding(), 2), equalTo(new GoldenUpdater.Result(2, 0, 2, 0, 0, 1, 0)));
        assertThat(manifests.size(), equalTo(0));
    }

    public void testNothingIsMadeWhenNothingIsStored() {
        assertThat(update(adding(), 10), equalTo(new GoldenUpdater.Result(0, 0, 0, 0, 0, 0, 0)));
        assertThat(manifests.size(), equalTo(0));
    }

    public void testAQueryIsInTheVersionOnceWhicheverSamplersPickedIt() throws IOException {
        stored = List.of(sample("second", 1, 300), sample("first", 1, 100), sample("first", 2, 50));

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

    public void testEnoughIsReadToGetPastWhatIsInTheVersionAlready() throws IOException {
        latestVersion = 2;
        golden = List.of(goldenRecord(1, 100, STORED), goldenRecord(2, 200, STORED));

        update(adding(), 5);

        assertThat("the limit and what is there", sampleSearches.get(0).source().size(), equalTo(7));
        assertThat("what is there is read up to the limit", goldenSearches.get(1).source().size(), equalTo(5));
    }

    public void testNotMoreThanASearchCanReturnIsRead() throws IOException {
        latestVersion = 2;
        golden = List.of(goldenRecord(1, 100, STORED));

        update(adding(), 10_000);

        assertThat(sampleSearches.get(0).source().size(), equalTo(10_000));
    }

    public void testAFailedReadIsReported() {
        failSampleRead = true;
        AtomicReference<Exception> failure = new AtomicReference<>();

        adding().update(10, ActionListener.wrap(result -> fail("expected a failure"), failure::set));

        assertThat(failure.get(), instanceOf(IllegalStateException.class));
        assertThat(manifests.size(), equalTo(0));
    }

    public void testWithoutAGoldenIndexTheFirstVersionIsMade() throws IOException {
        noGoldenIndex = true;
        stored = List.of(sample("s", 1, 100));

        assertThat(update(adding(), 10), equalTo(new GoldenUpdater.Result(0, 1, 0, 0, 1, 0, 0)));
    }

    // what is computed again

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
        assertThat(refreshed.contains("\"documents\":6"), equalTo(true));
        String copied = ((IndexRequest) bulks.get(0).requests().get(0)).source().utf8ToString();
        assertThat("as it was", copied.contains("\"documents\":5"), equalTo(true));
    }

    public void testWhatIsComputedAgainAndWhatIsAddedAreInTheSameVersion() throws IOException {
        latestVersion = 4;
        golden = List.of(goldenRecord(1, 100, STORED));
        now = n -> CHANGED;
        stored = List.of(sample("s", 2, 200));

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
        assertThat("nothing to compare, so no probe", probes.size(), equalTo(1));
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
        failingProbes = Set.of(2L); // cannot be looked at
        failingExact = Set.of(3L); // looked at, stale, and the scan fails

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

    public void testUnreadableRecordsAreCountedAsFailed() throws IOException {
        latestVersion = 4;
        SearchHit broken = SearchHit.unpooled(9, "broken");
        broken.sourceRef(new BytesArray("{\"kind\":\"record\"}"));
        golden = List.of(goldenRecord(1, 100, STORED), broken);
        now = n -> CHANGED;

        assertThat(update(refreshing(), 10), equalTo(new GoldenUpdater.Result(4, 5, 0, 1, 0, 0, 1)));
    }

    public void testTheExactSearchIsTheOneOfTheQuery() throws IOException {
        latestVersion = 4;
        golden = List.of(goldenRecord(1, 100, STORED));
        now = n -> CHANGED;

        update(refreshing(), 10);

        CapturedQuery query = new CapturedQuery(new String[] { "idx1" }, "vec", new float[] { 1f }, 3, 10, null, null, List.of(), null);
        assertThat(exactSearches.get(0).source().query(), equalTo(ExactSearch.request(query).source().query()));
    }

    public void testWhatIsComputedAgainIsSubjectToTheLimitToo() throws IOException {
        latestVersion = 4;
        golden = List.of(goldenRecord(1, 100, STORED), goldenRecord(2, 200, STORED));
        now = n -> CHANGED;
        stored = List.of(sample("s", 3, 300));

        GoldenUpdater.Result result = update(refreshing(), 2);

        assertThat("the oldest is left out, though it was computed again", result, equalTo(new GoldenUpdater.Result(4, 5, 0, 1, 1, 1, 0)));
        assertThat(writtenIds(), equalTo(List.of("5_" + fp(3), "5_" + fp(2))));
    }

    private static SearchHit manifest(long version) {
        SearchHit hit = SearchHit.unpooled(0, GoldenRecord.manifestId(version));
        hit.sourceRef(new BytesArray("{\"kind\":\"version\",\"dataset_version\":" + version + ",\"completed\":true}"));
        return hit;
    }

    private static StoredSample stored(long n, long pickedAt, String groundTruthId, DataState state) {
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

    /**
     * A record of the latest version of the golden dataset, of the query with the number {@code n}, picked at a time. The query is
     * searched in the index {@code idx<n>}, which is how the fakes tell the queries apart.
     */
    private static SearchHit goldenRecord(long n, long pickedAt, DataState state) throws IOException {
        SearchHit hit = SearchHit.unpooled((int) n, GoldenRecord.recordId(2, fp(n)));
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            hit.sourceRef(BytesReference.bytes(GoldenRecord.record(builder, 2, stored(n, pickedAt, "golden" + n, state), 9L)));
        }
        return hit;
    }

    /**
     * A stored sampled query with ground truth, picked at a time, as the search of the sample returns it.
     */
    private static SearchHit sample(String samplerId, long n, long pickedAt) throws IOException {
        QueryFingerprint fingerprint = new QueryFingerprint(n, n);
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", new float[] { n }, 3, 10, null, null, List.of(), null);
        SampledQuery sampled = new SampledQuery(
            fingerprint,
            new CapturedSearch(query, List.of(), 1, 1.0),
            new MultiplicityTracker(10).record(fingerprint)
        );
        sampled.attach(GroundTruth.KEY, new GroundTruth(List.of(new CapturedSearch.Hit("idx", "stored" + n, 1f))));
        SearchHit hit = SearchHit.unpooled((int) n, SampleRecord.documentId(samplerId, fingerprint));
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            hit.sourceRef(BytesReference.bytes(SampleRecord.document(builder, samplerId, sampled, pickedAt)));
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
