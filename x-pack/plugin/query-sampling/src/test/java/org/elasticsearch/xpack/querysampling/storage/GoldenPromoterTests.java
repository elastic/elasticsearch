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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
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
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class GoldenPromoterTests extends ESTestCase {

    private final List<SearchRequest> searches = new ArrayList<>();
    private final List<IndexRequest> indexRequests = new ArrayList<>();
    private final List<BulkRequest> bulks = new ArrayList<>();
    private final List<UpdateRequest> updates = new ArrayList<>();

    private List<SearchHit> stored = List.of();
    private Long latestVersion; // null: there is no golden index yet
    private int conflicts; // the next manifests that are refused because the number is taken
    private int failedRecords; // the first records of a bulk request that are rejected
    private boolean failBulk;

    private GoldenPromoter promoter() {
        return new GoldenPromoter((request, listener) -> {
            searches.add(request);
            if (request.indices()[0].equals(GoldenIndex.NAME)) {
                if (latestVersion == null) {
                    listener.onFailure(new IndexNotFoundException(GoldenIndex.NAME));
                } else {
                    respond(listener, latestVersion == 0 ? List.of() : List.of(manifest(latestVersion)));
                }
            } else {
                respond(listener, stored);
            }
        }, (request, listener) -> {
            indexRequests.add(request);
            if (conflicts > 0) {
                conflicts--;
                listener.onFailure(new VersionConflictEngineException(new ShardId(GoldenIndex.NAME, "_na_", 0), request.id(), "exists"));
            } else {
                listener.onResponse(new IndexResponse(new ShardId(GoldenIndex.NAME, "_na_", 0), request.id(), 0, 1, 1, true));
            }
        }, (request, listener) -> {
            bulks.add(request);
            if (failBulk) {
                listener.onFailure(new IllegalStateException("bulk failed"));
                return;
            }
            BulkItemResponse[] items = new BulkItemResponse[request.numberOfActions()];
            for (int i = 0; i < items.length; i++) {
                items[i] = i < failedRecords
                    ? BulkItemResponse.failure(
                        i,
                        DocWriteRequest.OpType.CREATE,
                        new BulkItemResponse.Failure(GoldenIndex.NAME, "id", new IllegalStateException("rejected"))
                    )
                    : BulkItemResponse.success(
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
        }, new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents()), () -> 42L);
    }

    private GoldenPromoter.Result promote(GoldenPromoter promoter, int max) {
        AtomicReference<GoldenPromoter.Result> result = new AtomicReference<>();
        promoter.promote(max, ActionListener.wrap(result::set, e -> fail(e)));
        return result.get();
    }

    public void testStoredQueriesAreCopiedToANewVersionAndTheVersionIsCompleted() throws IOException {
        stored = List.of(sample("sampler", 1), sample("sampler", 2));
        latestVersion = 0L;

        GoldenPromoter.Result result = promote(promoter(), 5);

        assertThat(result, equalTo(new GoldenPromoter.Result(1, 2, 0)));
        assertThat("the manifest takes the number first", indexRequests.get(0).id(), equalTo("version_1"));
        assertThat(indexRequests.get(0).opType(), equalTo(DocWriteRequest.OpType.CREATE));
        assertThat(indexRequests.get(0).source().utf8ToString().contains("\"completed\":false"), equalTo(true));
        assertThat(bulks.get(0).numberOfActions(), equalTo(2));
        List<String> ids = bulks.get(0).requests().stream().map(DocWriteRequest::id).toList();
        assertThat(ids, equalTo(List.of("1_" + new QueryFingerprint(1, 1).hex(), "1_" + new QueryFingerprint(2, 2).hex())));
        assertTrue(
            "a record is never overwritten",
            bulks.get(0).requests().stream().allMatch(r -> r.opType() == DocWriteRequest.OpType.CREATE)
        );
        assertThat(updates.size(), equalTo(1));
        assertThat(updates.get(0).id(), equalTo("version_1"));
        assertThat(updates.get(0).doc().source().utf8ToString(), equalTo("{\"records\":2,\"completed\":true}"));
    }

    public void testOnlyQueriesWithGroundTruthAndNotEventsAreRead() throws IOException {
        stored = List.of();

        promote(promoter(), 7);

        assertThat(searches.get(0).indices(), equalTo(new String[] { QuerySamplingIndex.NAME }));
        assertThat(searches.get(0).source().size(), equalTo(7));
        assertThat(
            searches.get(0).source().query(),
            equalTo(
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("has_ground_truth", true))
                    .mustNot(QueryBuilders.existsQuery("event_id"))
            )
        );
    }

    public void testNothingIsMadeWhenThereIsNothingToPromote() {
        stored = List.of();
        latestVersion = 3L;

        assertThat(promote(promoter(), 5), equalTo(new GoldenPromoter.Result(0, 0, 0)));
        assertThat("not even a version", indexRequests.size(), equalTo(0));
        assertThat(bulks.size(), equalTo(0));
    }

    public void testTheNextVersionFollowsTheLatest() throws IOException {
        stored = List.of(sample("sampler", 1));
        latestVersion = 4L;

        assertThat(promote(promoter(), 5).version(), equalTo(5L));
        assertThat(indexRequests.get(0).id(), equalTo("version_5"));
        assertThat(bulks.get(0).requests().get(0).id(), equalTo("5_" + new QueryFingerprint(1, 1).hex()));
    }

    public void testTheFirstVersionIsOneWhenThereIsNoGoldenIndexYet() throws IOException {
        stored = List.of(sample("sampler", 1));
        latestVersion = null;

        assertThat(promote(promoter(), 5).version(), equalTo(1L));
    }

    public void testANumberThatIsTakenInTheMeantimeMakesWayForTheNext() throws IOException {
        stored = List.of(sample("sampler", 1));
        latestVersion = 0L;
        conflicts = 2; // other promotions got versions 1 and 2

        GoldenPromoter.Result result = promote(promoter(), 5);

        assertThat(result.version(), equalTo(3L));
        assertThat(indexRequests.stream().map(IndexRequest::id).toList(), equalTo(List.of("version_1", "version_2", "version_3")));
        assertThat(bulks.get(0).requests().get(0).id(), equalTo("3_" + new QueryFingerprint(1, 1).hex()));
    }

    public void testGivesUpWhenTheNumbersKeepBeingTaken() throws IOException {
        stored = List.of(sample("sampler", 1));
        latestVersion = 0L;
        conflicts = 100;
        AtomicReference<Exception> failure = new AtomicReference<>();

        promoter().promote(5, ActionListener.wrap(result -> fail("expected a failure"), failure::set));

        assertThat(failure.get(), instanceOf(VersionConflictEngineException.class));
        assertThat("nothing is written to a version that was not made", bulks.size(), equalTo(0));
    }

    public void testAQueryIsInAVersionOnceAndTheMostRecentOneIsTaken() throws IOException {
        // the first of a fingerprint is the one that was picked last, they are read in that order
        SearchHit newest = sample("second", 1);
        SearchHit older = sample("first", 1);
        stored = List.of(newest, older, sample("first", 2));
        latestVersion = 0L;

        assertThat(promote(promoter(), 5), equalTo(new GoldenPromoter.Result(1, 2, 0)));
        assertThat(bulks.get(0).numberOfActions(), equalTo(2));
        assertThat(
            ((IndexRequest) bulks.get(0).requests().get(0)).source().utf8ToString().contains("\"sampler_id\":\"second\""),
            equalTo(true)
        );
    }

    public void testRejectedRecordsAreCountedAndTheVersionIsCompletedWithWhatWasWritten() throws IOException {
        stored = List.of(sample("sampler", 1), sample("sampler", 2), sample("sampler", 3));
        latestVersion = 0L;
        failedRecords = 1;

        assertThat(promote(promoter(), 5), equalTo(new GoldenPromoter.Result(1, 2, 1)));
        assertThat(updates.get(0).doc().source().utf8ToString(), equalTo("{\"records\":2,\"completed\":true}"));
    }

    public void testAFailedBulkRequestIsReportedAndTheVersionIsLeftIncomplete() throws IOException {
        stored = List.of(sample("sampler", 1));
        latestVersion = 0L;
        failBulk = true;
        AtomicReference<Exception> failure = new AtomicReference<>();

        promoter().promote(5, ActionListener.wrap(result -> fail("expected a failure"), failure::set));

        assertThat(failure.get(), instanceOf(IllegalStateException.class));
        assertThat("never completed, so no reader is to use it", updates.size(), equalTo(0));
    }

    private static SearchHit manifest(long version) {
        SearchHit hit = SearchHit.unpooled(0, GoldenRecord.manifestId(version));
        hit.sourceRef(new org.elasticsearch.common.bytes.BytesArray("{\"kind\":\"version\",\"dataset_version\":" + version + "}"));
        return hit;
    }

    /**
     * A stored query with a ground truth, as the search of the sample returns it.
     */
    private static SearchHit sample(String samplerId, long n) throws IOException {
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", new float[] { n }, 3, 10, null, null, List.of(), null);
        QueryFingerprint fingerprint = new QueryFingerprint(n, n);
        SampledQuery sampled = new SampledQuery(
            fingerprint,
            new CapturedSearch(query, List.of(), 1, 1.0),
            new MultiplicityTracker(10).record(fingerprint)
        );
        sampled.attach(GroundTruth.KEY, new GroundTruth(List.of(new CapturedSearch.Hit("idx", "d" + n, 1f))));
        SearchHit hit = SearchHit.unpooled((int) n, SampleRecord.documentId(samplerId, fingerprint));
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            hit.sourceRef(BytesReference.bytes(SampleRecord.document(builder, samplerId, sampled, 1L)));
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
