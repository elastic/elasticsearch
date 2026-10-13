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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class GoldenGrowerTests extends ESTestCase {

    private static final NamedXContentRegistry REGISTRY = new NamedXContentRegistry(
        new SearchModule(Settings.EMPTY, List.of()).getNamedXContents()
    );

    private final List<SearchRequest> sampleSearches = new ArrayList<>();
    private final List<SearchRequest> goldenSearches = new ArrayList<>();
    private final List<IndexRequest> manifests = new ArrayList<>();
    private final List<BulkRequest> bulks = new ArrayList<>();

    private List<SearchHit> stored = List.of(); // the sampled queries that have ground truth, newest first
    private List<SearchHit> golden = List.of(); // the records of the latest version
    private long latestVersion = 0;
    private boolean failSampleRead;

    private GoldenGrower grower() {
        GoldenReader reader = new GoldenReader((request, listener) -> {
            goldenSearches.add(request);
            if (request.source().size() == 1) {
                respond(listener, latestVersion == 0 ? List.of() : List.of(manifest(latestVersion)));
            } else {
                respond(listener, golden);
            }
        }, REGISTRY);
        StoredSamples samples = new StoredSamples((request, listener) -> {
            sampleSearches.add(request);
            if (failSampleRead) {
                listener.onFailure(new IllegalStateException("read failed"));
            } else {
                respond(listener, stored);
            }
        }, REGISTRY);
        GoldenWriter writer = new GoldenWriter(
            (request, listener) -> respond(listener, latestVersion == 0 ? List.of() : List.of(manifest(latestVersion))),
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
            (request, listener) -> listener.onResponse(
                new UpdateResponse(new ShardId(GoldenIndex.NAME, "_na_", 0), request.id(), 0, 1, 1, DocWriteResponse.Result.UPDATED)
            ),
            () -> 42L
        );
        return new GoldenGrower(reader, samples, writer);
    }

    private GoldenGrower.Result grow(GoldenGrower grower, int limit) {
        AtomicReference<GoldenGrower.Result> result = new AtomicReference<>();
        grower.grow(limit, ActionListener.wrap(result::set, e -> fail(e)));
        return result.get();
    }

    private static String fp(long n) {
        return new QueryFingerprint(n, n).hex();
    }

    private List<String> promotedIds() {
        return bulks.get(0).requests().stream().map(DocWriteRequest::id).toList();
    }

    public void testWithoutAGoldenDatasetTheStoredQueriesMakeTheFirstVersion() throws IOException {
        stored = List.of(sample("s", 3, 300), sample("s", 2, 200), sample("s", 1, 100));

        GoldenGrower.Result result = grow(grower(), 10);

        assertThat(result, equalTo(new GoldenGrower.Result(0, 1, 3, 0, 0)));
        assertThat(manifests.get(0).id(), equalTo("version_1"));
        assertThat("it is not made from another", manifests.get(0).source().utf8ToString().contains("source_version"), equalTo(false));
        assertThat(bulks.get(0).numberOfActions(), equalTo(3));
    }

    public void testOnlyQueriesThatAreNotInTheLatestVersionAreAddedAndTheOnesThereAreKept() throws IOException {
        latestVersion = 2;
        golden = List.of(goldenRecord(1, 100), goldenRecord(2, 200));
        stored = List.of(sample("s", 4, 400), sample("s", 3, 300), sample("s", 2, 200), sample("s", 1, 100));

        GoldenGrower.Result result = grow(grower(), 10);

        assertThat(result, equalTo(new GoldenGrower.Result(2, 3, 2, 0, 0)));
        assertThat(manifests.get(0).source().utf8ToString().contains("\"source_version\":2"), equalTo(true));
        assertThat(
            "the newest first, and what was there is a copy",
            promotedIds(),
            equalTo(List.of("3_" + fp(4), "3_" + fp(3), "3_" + fp(2), "3_" + fp(1)))
        );
    }

    public void testWhatWasInTheVersionKeepsItsGroundTruth() throws IOException {
        latestVersion = 2;
        golden = List.of(goldenRecord(1, 100));
        stored = List.of(sample("s", 2, 200), sample("s", 1, 100));

        grow(grower(), 10);

        String copy = ((IndexRequest) bulks.get(0).requests().get(1)).source().utf8ToString();
        assertThat("not the one of the stored query, which would be of other data", copy.contains("\"id\":\"golden1\""), equalTo(true));
    }

    public void testWhenThereAreMoreThanTheLimitTheOnesPickedLongestAgoAreLeftOut() throws IOException {
        latestVersion = 2;
        golden = List.of(goldenRecord(1, 100), goldenRecord(2, 200));
        stored = List.of(sample("s", 4, 400), sample("s", 3, 300));

        GoldenGrower.Result result = grow(grower(), 3);

        assertThat(result, equalTo(new GoldenGrower.Result(2, 3, 2, 1, 0)));
        assertThat(promotedIds(), equalTo(List.of("3_" + fp(4), "3_" + fp(3), "3_" + fp(2))));
    }

    public void testNoVersionIsMadeWhenNothingIsNew() throws IOException {
        latestVersion = 2;
        golden = List.of(goldenRecord(1, 100), goldenRecord(2, 200));
        stored = List.of(sample("s", 2, 200), sample("s", 1, 100));

        assertThat(grow(grower(), 10), equalTo(new GoldenGrower.Result(2, 0, 0, 0, 0)));
        assertThat(manifests.size(), equalTo(0));
        assertThat(bulks.size(), equalTo(0));
    }

    public void testNoVersionIsMadeWhenWhatIsNewIsOlderThanAllThatIsThereAndTheVersionIsFull() throws IOException {
        latestVersion = 2;
        golden = List.of(goldenRecord(2, 200), goldenRecord(3, 300));
        stored = List.of(sample("s", 1, 100));

        assertThat("it would not change", grow(grower(), 2), equalTo(new GoldenGrower.Result(2, 0, 0, 1, 0)));
        assertThat(manifests.size(), equalTo(0));
    }

    public void testNothingIsPromotedWhenNothingIsStored() {
        assertThat(grow(grower(), 10), equalTo(new GoldenGrower.Result(0, 0, 0, 0, 0)));
        assertThat(manifests.size(), equalTo(0));
    }

    public void testAQueryIsInTheVersionOnceWhicheverSamplersPickedIt() throws IOException {
        stored = List.of(sample("second", 1, 300), sample("first", 1, 100), sample("first", 2, 50));

        GoldenGrower.Result result = grow(grower(), 10);

        assertThat(result.added(), equalTo(2));
        assertThat(
            ((IndexRequest) bulks.get(0).requests().get(0)).source().utf8ToString().contains("\"sampler_id\":\"second\""),
            equalTo(true)
        );
    }

    public void testOnlyQueriesWithGroundTruthAndNotEventsAreRead() {
        grow(grower(), 10);

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
        golden = List.of(goldenRecord(1, 100), goldenRecord(2, 200));

        grow(grower(), 5);

        assertThat("the limit and what is there", sampleSearches.get(0).source().size(), equalTo(7));
        assertThat("what is there is read up to the limit", goldenSearches.get(1).source().size(), equalTo(5));
    }

    public void testNotMoreThanASearchCanReturnIsRead() throws IOException {
        latestVersion = 2;
        golden = List.of(goldenRecord(1, 100), goldenRecord(2, 200));

        grow(grower(), 10_000);

        assertThat(sampleSearches.get(0).source().size(), equalTo(10_000));
    }

    public void testAFailedReadIsReported() {
        failSampleRead = true;
        AtomicReference<Exception> failure = new AtomicReference<>();

        grower().grow(10, ActionListener.wrap(result -> fail("expected a failure"), failure::set));

        assertThat(failure.get(), instanceOf(IllegalStateException.class));
        assertThat(manifests.size(), equalTo(0));
    }

    public void testWithoutAGoldenIndexTheFirstVersionIsMade() throws IOException {
        stored = List.of(sample("s", 1, 100));
        GoldenReader noIndex = new GoldenReader(
            (request, listener) -> listener.onFailure(new IndexNotFoundException(GoldenIndex.NAME)),
            REGISTRY
        );
        GoldenGrower grower = new GoldenGrower(
            noIndex,
            new StoredSamples((request, listener) -> respond(listener, stored), REGISTRY),
            new GoldenWriter(
                (request, listener) -> listener.onFailure(new IndexNotFoundException(GoldenIndex.NAME)),
                (request, listener) -> {
                    manifests.add(request);
                    listener.onResponse(new IndexResponse(new ShardId(GoldenIndex.NAME, "_na_", 0), request.id(), 0, 1, 1, true));
                },
                (request, listener) -> listener.onResponse(
                    new BulkResponse(
                        new BulkItemResponse[] {
                            BulkItemResponse.success(
                                0,
                                DocWriteRequest.OpType.CREATE,
                                new IndexResponse(new ShardId(GoldenIndex.NAME, "_na_", 0), "id", 0, 1, 1, true)
                            ) },
                        1
                    )
                ),
                (request, listener) -> listener.onResponse(
                    new UpdateResponse(new ShardId(GoldenIndex.NAME, "_na_", 0), request.id(), 0, 1, 1, DocWriteResponse.Result.UPDATED)
                ),
                () -> 42L
            )
        );

        assertThat(grow(grower, 10), equalTo(new GoldenGrower.Result(0, 1, 1, 0, 0)));
    }

    private static SearchHit manifest(long version) {
        SearchHit hit = SearchHit.unpooled(0, GoldenRecord.manifestId(version));
        hit.sourceRef(new BytesArray("{\"kind\":\"version\",\"dataset_version\":" + version + ",\"completed\":true}"));
        return hit;
    }

    private static StoredSample stored(long n, long pickedAt, String groundTruthId) {
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", new float[] { n }, 3, 10, null, null, List.of(), null);
        return new StoredSample(
            "sampler",
            fp(n),
            new CapturedSearch(query, List.of(), 1, 1.0),
            new TrackedQuery.Weights(1, 1.0, 1.0, 1.0, 1.0),
            pickedAt,
            pickedAt,
            new GroundTruth(List.of(new CapturedSearch.Hit("idx", groundTruthId, 1f))),
            null,
            null,
            null,
            null
        );
    }

    /**
     * A record of the latest version of the golden dataset, of the query with the number {@code n}, picked at a time.
     */
    private static SearchHit goldenRecord(long n, long pickedAt) throws IOException {
        SearchHit hit = SearchHit.unpooled((int) n, GoldenRecord.recordId(2, fp(n)));
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            hit.sourceRef(BytesReference.bytes(GoldenRecord.record(builder, 2, stored(n, pickedAt, "golden" + n), 9L)));
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
    private static void respond(ActionListener<SearchResponse> listener, List<SearchHit> hits) {
        SearchHits searchHits = new SearchHits(hits.toArray(SearchHit[]::new), new TotalHits(hits.size(), TotalHits.Relation.EQUAL_TO), 1f);
        SearchResponse response = SearchResponseUtils.successfulResponse(searchHits);
        searchHits.decRef(); // the response holds its own reference
        try {
            listener.onResponse(response);
        } finally {
            response.decRef();
        }
    }
}
