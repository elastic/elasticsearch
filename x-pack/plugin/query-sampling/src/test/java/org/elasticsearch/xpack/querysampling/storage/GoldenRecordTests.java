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
import org.elasticsearch.xpack.querysampling.dedup.Selectivity;
import org.elasticsearch.xpack.querysampling.dedup.Stratum;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.DataState;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class GoldenRecordTests extends ESTestCase {

    private static StoredSample sample(Stratum stratum, Hardness hardness, Selectivity selectivity) {
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
        GroundTruth groundTruth = new GroundTruth(List.of(new CapturedSearch.Hit("a", "d1", 1f)), new DataState(5, 10.0));
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

    @SuppressWarnings("unchecked") // the document is JSON whose shape this class defines
    public void testRecordIsACopyOfTheStoredSampleWithItsGroundTruthAndTheVersion() throws IOException {
        StoredSample sample = sample(new Stratum("vec/2", 3), Hardness.HARD, Selectivity.LOW);

        Map<String, Object> record = toMap(GoldenRecord.record(JsonXContent.contentBuilder(), 4, sample, 9000L));

        assertThat(record.get("kind"), equalTo("record"));
        assertThat(record.get("dataset_version"), equalTo(4));
        assertThat(record.get("promoted_at"), equalTo(9000));
        assertThat("when it was picked, and not when it was written last", record.get("source_picked_at"), equalTo(1000));
        assertThat(record.get("sampler_id"), equalTo("s1"));
        assertThat(record.get("fingerprint"), equalTo(sample.fingerprint()));
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
        assertThat(groundTruth.get("data_state"), equalTo(Map.of("documents", 5, "seq_no_sum", 10.0)));
        assertThat("it is not a sample, and has nothing to refresh", record, not(hasKey("has_ground_truth")));
        assertThat(record, not(hasKey("updated_at")));
    }

    public void testWhatIsNotKnownOfAQueryIsLeftOut() throws IOException {
        Map<String, Object> record = toMap(GoldenRecord.record(JsonXContent.contentBuilder(), 1, sample(null, null, null), 9000L));

        assertThat(record, not(hasKey("spatial_cluster")));
        assertThat(record, not(hasKey("spatial_space")));
        assertThat(record, not(hasKey("hardness")));
        assertThat(record, not(hasKey("selectivity")));
    }

    public void testRecordsAreReadBackAsTheSamplesTheyWereCopiedFrom() throws IOException {
        StoredSample sample = sample(new Stratum("vec/2", 3), Hardness.HARD, Selectivity.LOW);

        Map<String, Object> record = toMap(GoldenRecord.record(JsonXContent.contentBuilder(), 4, sample, 9000L));
        StoredSample read = GoldenRecord.parse(
            record,
            new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents())
        );

        assertThat(read.samplerId(), equalTo(sample.samplerId()));
        assertThat(read.fingerprint(), equalTo(sample.fingerprint()));
        assertThat(read.weights(), equalTo(sample.weights()));
        assertThat(read.groundTruth(), equalTo(sample.groundTruth()));
        assertThat(read.stratum(), equalTo(sample.stratum()));
        assertThat(read.hardness(), equalTo(sample.hardness()));
        assertThat(read.selectivity(), equalTo(sample.selectivity()));
        assertThat(read.search().hits(), equalTo(sample.search().hits()));
        assertThat("when it was picked", read.pickedAt(), equalTo(1000L));
        assertThat("when it was promoted", read.updatedAt(), equalTo(9000L));
        assertThat(read.search().query().field(), equalTo("vec"));
        assertThat(read.search().query().filters(), equalTo(sample.search().query().filters()));
    }

    public void testManifestTellsTheVersionAndWhetherItIsComplete() throws IOException {
        Map<String, Object> manifest = toMap(GoldenRecord.manifest(JsonXContent.contentBuilder(), 4, 100L, 0, false));
        Map<String, Object> completion = toMap(GoldenRecord.completion(JsonXContent.contentBuilder(), 7));

        assertThat(manifest, equalTo(Map.of("kind", "version", "dataset_version", 4, "created_at", 100, "records", 0, "completed", false)));
        assertThat(completion, equalTo(Map.of("records", 7, "completed", true)));
    }

    public void testIdsAreOfTheVersionAndOfTheQuery() {
        assertThat(GoldenRecord.recordId(4, "abc"), equalTo("4_abc"));
        assertThat(GoldenRecord.manifestId(4), equalTo("version_4"));
        assertThat("the same query is a different record in another version", GoldenRecord.recordId(5, "abc"), not(equalTo("4_abc")));
    }

    @SuppressWarnings("unchecked") // the mappings are known to be nested maps, they are built by GoldenIndex
    public void testEveryFieldOfADocumentIsInTheMappings() throws IOException {
        Set<String> written = new HashSet<>();
        written.addAll(
            toMap(
                GoldenRecord.record(JsonXContent.contentBuilder(), 1, sample(new Stratum("vec/2", 0), Hardness.EASY, Selectivity.HIGH), 1L)
            ).keySet()
        );
        written.addAll(toMap(GoldenRecord.manifest(JsonXContent.contentBuilder(), 1, 1L, 0, false)).keySet());
        Map<String, Object> root = XContentHelper.convertToMap(BytesReference.bytes(GoldenIndex.mappings()), false, XContentType.JSON).v2();
        Map<String, Object> mapping = (Map<String, Object>) root.get("_doc");

        // the mappings are strict: a field they do not list would make the write fail, and one that is never written is dead
        assertThat(((Map<String, Object>) mapping.get("properties")).keySet(), equalTo(written));
    }

    private static Map<String, Object> toMap(XContentBuilder builder) {
        return XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
    }
}
