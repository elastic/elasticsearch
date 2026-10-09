/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.Hardness;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.Stratum;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
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
        assertThat("a query that was not put anywhere says nothing of it", document, not(hasKey("spatial_cluster")));
        assertThat(document, not(hasKey("spatial_space")));
        assertThat(document, not(hasKey("hardness")));
    }

    public void testTheStrataOfAQueryAreStoredAndReadBack() throws IOException {
        CapturedQuery query = new CapturedQuery(new String[] { "a" }, "vec", new float[] { 1f }, 10, 100, null, null, List.of(), null);
        TrackedQuery tracked = tracked(1.0);
        Stratum stratum = new Stratum("vec/1", randomIntBetween(0, 99));
        Hardness hardness = randomFrom(Hardness.values());
        tracked.stratum(stratum);
        tracked.hardness(hardness);
        SampledQuery sampled = new SampledQuery(FINGERPRINT, new CapturedSearch(query, List.of(), 1, 1.0), tracked);

        Map<String, Object> document = toMap(SampleRecord.document(JsonXContent.contentBuilder(), "s1", sampled, 5L));
        StoredSample stored = SampleRecord.parse(document, xContentRegistry());

        assertThat(document.get("hardness"), equalTo(hardness.name().toLowerCase(Locale.ROOT)));
        assertThat(stored.stratum(), equalTo(stratum));
        assertThat(stored.hardness(), equalTo(hardness));
    }

    public void testQueriesWithoutStrataAreReadBackWithoutThem() throws IOException {
        CapturedQuery query = new CapturedQuery(new String[] { "a" }, "vec", new float[] { 1f }, 10, 100, null, null, List.of(), null);
        SampledQuery sampled = new SampledQuery(FINGERPRINT, new CapturedSearch(query, List.of(), 1, 1.0), tracked(1.0));

        StoredSample stored = SampleRecord.parse(
            toMap(SampleRecord.document(JsonXContent.contentBuilder(), "s1", sampled, 5L)),
            xContentRegistry()
        );

        assertNull(stored.stratum());
        assertNull(stored.hardness());
    }

    @SuppressWarnings("unchecked") // as above
    public void testGroundTruthIsStoredOnceKnownAndOptionalPartsAreLeftOut() throws IOException {
        CapturedQuery query = new CapturedQuery(new String[] { "a" }, "vec", new float[] { 1f }, 10, 100, null, null, List.of(), null);
        SampledQuery sampled = new SampledQuery(FINGERPRINT, new CapturedSearch(query, List.of(), 1, 1.0), tracked(1.0));
        sampled.attach(GroundTruth.KEY, new GroundTruth(List.of(new CapturedSearch.Hit("a", "d2", 1f))));

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
        TrackedQuery tracked = tracked(1.0);
        tracked.stratum(new Stratum("vec/1", 3));
        tracked.hardness(Hardness.HARD);
        SampledQuery sampled = new SampledQuery(FINGERPRINT, new CapturedSearch(query, List.of(), 1, 1.0), tracked);
        sampled.attach(GroundTruth.KEY, new GroundTruth(List.of()));

        Map<String, Object> document = toMap(SampleRecord.document(JsonXContent.contentBuilder(), "s1", sampled, 5L));

        // the mappings are strict: a field they do not list would make the write fail
        assertThat(mappedFields(), equalTo(document.keySet()));
    }

    public void testADocumentCanBeReadBack() throws IOException {
        float[] vector = { randomFloat(), randomFloat(), -randomFloat() };
        CapturedQuery query = new CapturedQuery(
            new String[] { "a", "b" },
            "vec",
            vector,
            randomIntBetween(1, 100),
            randomIntBetween(100, 1000),
            randomBoolean() ? null : randomFloat(),
            randomBoolean() ? null : randomFloat(),
            randomBoolean()
                ? List.of()
                : List.of(
                    QueryBuilders.termQuery("category", randomIntBetween(1, 9)),
                    QueryBuilders.boolQuery().should(QueryBuilders.rangeQuery("price").gte(10)).should(QueryBuilders.existsQuery("brand"))
                ),
            randomBoolean() ? null : "q" + randomInt()
        );
        List<CapturedSearch.Hit> hits = List.of(new CapturedSearch.Hit("a", "d1", randomFloat()), new CapturedSearch.Hit("b", "d2", 0.5f));
        double captureRate = randomDoubleBetween(0.01, 1.0, true);
        SampledQuery sampled = new SampledQuery(
            FINGERPRINT,
            new CapturedSearch(query, hits, randomNonNegativeLong() % 1000, captureRate),
            tracked(captureRate)
        );
        if (randomBoolean()) {
            sampled.attach(GroundTruth.KEY, new GroundTruth(List.of(new CapturedSearch.Hit("a", "d3", 1f))));
        }

        StoredSample stored = SampleRecord.parse(
            toMap(SampleRecord.document(JsonXContent.contentBuilder(), "s1", sampled, 1000L)),
            xContentRegistry()
        );

        CapturedQuery read = stored.search().query();
        assertArrayEquals(query.indices(), read.indices());
        assertThat(read.field(), equalTo(query.field()));
        assertArrayEquals(query.queryVector(), read.queryVector(), 0f);
        assertThat(read.k(), equalTo(query.k()));
        assertThat(read.numCandidates(), equalTo(query.numCandidates()));
        assertThat(read.visitPercentage(), equalTo(query.visitPercentage()));
        assertThat(read.oversample(), equalTo(query.oversample()));
        assertThat(read.filters(), equalTo(query.filters()));
        assertThat(read.opaqueId(), equalTo(query.opaqueId()));
        assertThat(stored.search().hits(), equalTo(hits));
        assertThat(stored.search().tookMillis(), equalTo(sampled.search().tookMillis()));
        // the rate is stored as the inverse of the weight of the arrival, which can be off in the last digit
        assertThat(stored.search().captureRate(), closeTo(captureRate, 1e-12));
        assertThat(stored.weights(), equalTo(sampled.tracked().weights()));
        assertThat(stored.samplerId(), equalTo("s1"));
        assertThat(stored.fingerprint(), equalTo(FINGERPRINT.hex()));
        assertThat(stored.pickedAt(), equalTo(1000L));
        assertThat(stored.groundTruth(), equalTo(sampled.attachment(GroundTruth.KEY)));
        assertThat("it can be found again by its id", stored.id(), equalTo(SampleRecord.documentId("s1", FINGERPRINT)));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents());
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
