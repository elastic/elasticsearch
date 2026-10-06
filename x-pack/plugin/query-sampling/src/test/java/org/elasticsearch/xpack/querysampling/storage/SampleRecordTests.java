/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class SampleRecordTests extends ESTestCase {

    private static final QueryFingerprint FINGERPRINT = new QueryFingerprint(1L, -1L);

    public void testDocumentIdTellsSamplersApart() {
        assertThat(SampleRecord.documentId("a", FINGERPRINT), equalTo("a_0000000000000001ffffffffffffffff"));
        assertThat(SampleRecord.documentId("b", FINGERPRINT), not(equalTo(SampleRecord.documentId("a", FINGERPRINT))));
    }

    @SuppressWarnings("unchecked") // the document is JSON whose shape this class defines
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
    }

    @SuppressWarnings("unchecked") // as above
    public void testGroundTruthIsStoredOnceKnownAndOptionalPartsAreLeftOut() throws IOException {
        CapturedQuery query = new CapturedQuery(new String[] { "a" }, "vec", new float[] { 1f }, 10, 100, null, null, List.of(), null);
        SampledQuery sampled = new SampledQuery(FINGERPRINT, new CapturedSearch(query, List.of(), 1, 1.0), tracked(1.0));
        sampled.groundTruth(new GroundTruth(List.of(new CapturedSearch.Hit("a", "d2", 1f))));

        Map<String, Object> document = toMap(SampleRecord.document(JsonXContent.contentBuilder(), "s1", sampled, 5L));

        assertThat(document.get("has_ground_truth"), equalTo(true));
        assertThat(((List<?>) ((Map<String, Object>) document.get("ground_truth")).get("neighbors")).size(), equalTo(1));
        Map<String, Object> storedQuery = (Map<String, Object>) document.get("query");
        assertThat(storedQuery, not(hasKey("visit_percentage")));
        assertThat(storedQuery, not(hasKey("oversample")));
        assertThat(storedQuery, not(hasKey("opaque_id")));
    }

    public void testEveryFieldOfADocumentIsInTheMappings() throws IOException {
        CapturedQuery query = new CapturedQuery(new String[] { "a" }, "vec", new float[] { 1f }, 10, 100, 0.5f, 3f, List.of(), "q");
        SampledQuery sampled = new SampledQuery(FINGERPRINT, new CapturedSearch(query, List.of(), 1, 1.0), tracked(1.0));
        sampled.groundTruth(new GroundTruth(List.of()));

        Map<String, Object> document = toMap(SampleRecord.document(JsonXContent.contentBuilder(), "s1", sampled, 5L));

        // the mappings are strict: a field they do not list would make the write fail
        assertThat(mappedFields(), equalTo(document.keySet()));
    }

    public void testWeightsUpdateOnlyHasWhatChanges() throws IOException {
        TrackedQuery tracked = tracked(0.5);

        Map<String, Object> update = toMap(SampleRecord.weightsUpdate(JsonXContent.contentBuilder(), tracked.weights(), 99L));

        assertThat(
            update.keySet(),
            equalTo(
                Set.of("multiplicity", "weighted_multiplicity", "inclusion_probability", "seen_probability", "capture_rate", "updated_at")
            )
        );
        assertThat(update.get("updated_at"), equalTo(99));
    }

    @SuppressWarnings("unchecked") // the mappings are known to be nested maps, they are built by QuerySamplingIndex
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
}
