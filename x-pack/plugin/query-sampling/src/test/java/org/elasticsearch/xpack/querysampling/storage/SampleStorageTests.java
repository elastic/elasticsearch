/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.Hardness;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.Selectivity;
import org.elasticsearch.xpack.querysampling.dedup.Stratum;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class SampleStorageTests extends ESTestCase {

    private static final QueryFingerprint FINGERPRINT = new QueryFingerprint(1L, -1L);

    @SuppressWarnings("unchecked")
    public void testDocumentHoldsTheQueryTheAnswerAndTheWeights() throws IOException {
        TrackedQuery tracked = tracked(0.25);
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
        SampledQuery sampled = new SampledQuery(
            FINGERPRINT,
            new CapturedSearch(query, List.of(new CapturedSearch.Hit("a", "d1", 0.9f)), 12, 0.25),
            tracked
        );

        Map<String, Object> document = toMap(SampleRecord.document(JsonXContent.contentBuilder(), "s1", sampled, 1000L));

        assertThat(document.get("sampler_id"), equalTo("s1"));
        assertThat(document.get("fingerprint"), equalTo(FINGERPRINT.hex()));
        assertThat(document.get("indices"), equalTo(List.of("a", "b")));
        assertThat(document.get("field"), equalTo("vec"));
        assertThat(document.get("k"), equalTo(10));
        assertThat(document.get("picked_at"), equalTo(1000));
        assertThat(document.get("updated_at"), equalTo(1000));
        assertThat(document.get("has_ground_truth"), equalTo(false));
        assertThat(document.get("capture_rate"), equalTo(0.25));
        assertThat(document.get("weighted_multiplicity"), equalTo(4.0));
        assertThat(document.get("multiplicity"), equalTo(1));
        assertThat(((Number) document.get("inclusion_probability")).doubleValue(), closeTo(tracked.inclusionProbability(), 1e-12));

        Map<String, Object> storedQuery = (Map<String, Object>) document.get("query");
        assertThat(storedQuery.get("query_vector"), equalTo(List.of(1.0, 2.0)));
        assertThat(storedQuery.get("num_candidates"), equalTo(100));
        assertThat(storedQuery.get("visit_percentage"), equalTo(0.5));
        assertThat(storedQuery.get("oversample"), equalTo(3.0));
        assertThat(storedQuery.get("opaque_id"), equalTo("q7"));
        assertThat(((List<?>) storedQuery.get("filters")).size(), equalTo(1));

        Map<String, Object> liveHits = (Map<String, Object>) document.get("live_hits");
        assertThat(liveHits.get("took_millis"), equalTo(12));
        assertThat(((List<?>) liveHits.get("hits")).size(), equalTo(1));
        assertThat(document, not(hasKey("ground_truth")));
        assertThat("a query that was not put anywhere says nothing of it", document, not(hasKey("spatial_cluster")));
        assertThat(document, not(hasKey("spatial_space")));
        assertThat(document, not(hasKey("hardness")));
        assertThat(document, not(hasKey("selectivity")));
        assertThat("a picked query is not an event", document, not(hasKey("event_id")));
    }

    public void testEveryFieldOfADocumentIsInTheMappings() throws IOException {
        CapturedQuery query = new CapturedQuery(new String[] { "a" }, "vec", new float[] { 1f }, 10, 100, 0.5f, 3f, List.of(), "q");
        TrackedQuery tracked = tracked(1.0);
        tracked.stratum(new Stratum("vec/1", 3));
        tracked.hardness(Hardness.HARD);
        tracked.selectivity(Selectivity.LOW);
        SampledQuery sampled = SampledQuery.event(FINGERPRINT, new CapturedSearch(query, List.of(), 1, 1.0), tracked, "e1");
        sampled.attach(GroundTruth.KEY, new GroundTruth(List.of()));

        Map<String, Object> document = toMap(SampleRecord.document(JsonXContent.contentBuilder(), "s1", sampled, 5L));

        assertThat(mappedFields(), equalTo(document.keySet()));
    }

    @SuppressWarnings("unchecked")
    private static Set<String> mappedFields() {
        Map<String, Object> root = XContentHelper.convertToMap(
            BytesReference.bytes(QuerySamplingIndex.mappings()),
            false,
            XContentType.JSON
        ).v2();
        Map<String, Object> mapping = (Map<String, Object>) root.get("_doc");
        return ((Map<String, Object>) mapping.get("properties")).keySet();
    }

    private static TrackedQuery tracked(double captureRate) {
        return new MultiplicityTracker(10).record(FINGERPRINT, captureRate);
    }

    private static Map<String, Object> toMap(XContentBuilder builder) {
        return XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
    }

    private final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();

    private final List<BulkRequest> requests = new ArrayList<>();

    private final List<ActionListener<BulkResponse>> listeners = new ArrayList<>();

    private static final TimeValue REFRESH_INTERVAL = TimeValue.timeValueSeconds(30);

    private final MultiplicityTracker tracker = new MultiplicityTracker(100);

    public void testChangedWeightsAreWrittenAgainAndUnchangedOnesAreNot() {
        WeightsRefresher refresher = refresher(10);
        QueryFingerprint fingerprint = new QueryFingerprint(1, 1);
        TrackedQuery query = tracker.record(fingerprint);
        refresher.written(fingerprint, query, query.weights());

        nextRound();
        assertThat(requests.size(), equalTo(0));

        tracker.record(fingerprint);
        nextRound();

        assertThat(requests.size(), equalTo(1));
        assertThat(requests.get(0).numberOfActions(), equalTo(1));
        assertThat(requests.get(0).requests().get(0).id(), equalTo(SampleRecord.documentId("sampler", fingerprint)));
        listeners.get(0).onResponse(responseOf(1));
        assertThat(refresher.refreshed(), equalTo(1L));

        nextRound();
        assertThat(requests.size(), equalTo(1));
        assertThat(refresher.tracked(), equalTo(1));
    }

    private WeightsRefresher refresher(int maxBatch) {
        return new WeightsRefresher("sampler", (request, listener) -> {
            requests.add(request);
            listeners.add(listener);
        }, taskQueue.getThreadPool(), taskQueue.getThreadPool().generic(), () -> 42L, (fp, query) -> true, maxBatch, REFRESH_INTERVAL);
    }

    private void nextRound() {
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
    }

    private static BulkResponse responseOf(int items) {
        BulkItemResponse[] responses = new BulkItemResponse[items];
        for (int i = 0; i < items; i++) {
            responses[i] = BulkItemResponse.success(
                i,
                DocWriteRequest.OpType.UPDATE,
                new UpdateResponse(new ShardId(QuerySamplingIndex.NAME, "_na_", 0), "id", 0, 1, 1, DocWriteResponse.Result.UPDATED)
            );
        }
        return new BulkResponse(responses, 1);
    }
}
