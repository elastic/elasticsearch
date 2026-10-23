/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.elasticsearch.search.vectors.RescoreVectorBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

public class KnnEvalRescoreTests extends ESTestCase {

    private static KnnEvalRescore rescoreFor(String indexType) {
        return KnnEvalRescore.fromFieldMapping(
            "emb",
            Map.of("type", "dense_vector", "index_options", Map.of("type", indexType, "rescore_vector", Map.of("oversample", 3.0)))
        );
    }

    /** Unlike {@link #rescoreFor}, the mapping has no oversample, so nothing rescores unless the request says so. */
    private static KnnEvalRescore withoutMappingOversample(String indexType) {
        return KnnEvalRescore.fromFieldMapping("emb", Map.of("type", "dense_vector", "index_options", Map.of("type", indexType)));
    }

    public void testDiskBbqAndHnswFamilyAreSupported() {
        for (String indexType : List.of("bbq_disk", "hnsw", "int8_hnsw", "int4_hnsw", "bbq_hnsw")) {
            assertNotNull(rescoreFor(indexType));
        }
        // flat types scan every vector, so there is no search effort to evaluate; unknown types are reported alike
        for (String indexType : List.of("flat", "int8_flat", "int4_flat", "bbq_flat", "made_up")) {
            assertThat(
                expectThrows(
                    IllegalArgumentException.class,
                    () -> KnnEvalRescore.fromFieldMapping("emb", Map.of("type", "dense_vector", "index_options", Map.of("type", indexType)))
                ).getMessage(),
                containsString(
                    "field [emb] must use [index_options.type] of [bbq_disk, hnsw, int8_hnsw, int4_hnsw, bbq_hnsw], found ["
                        + indexType
                        + "]"
                )
            );
        }
    }

    public void testExplicitIndexOptionsAreRequired() {
        for (Map<String, Object> mapping : Arrays.<Map<String, Object>>asList(Map.of("type", "dense_vector"), null)) {
            assertThat(
                expectThrows(IllegalArgumentException.class, () -> KnnEvalRescore.fromFieldMapping("emb", mapping)).getMessage(),
                allOf(containsString("field [emb] must use [index_options.type] of [bbq_disk"), containsString("found [null]"))
            );
        }
    }

    private static final List<String> GRAPH_TYPES = List.of("hnsw", "int8_hnsw", "int4_hnsw", "bbq_hnsw");
    private static final List<String> QUANTIZED_GRAPH_TYPES = List.of("int8_hnsw", "int4_hnsw", "bbq_hnsw");

    private static float randomVisitPercentage() {
        return randomFloatBetween(0.0f, 100.0f, true);
    }

    private static int randomNumCandidates() {
        return randomIntBetween(1, KnnEvalRescore.MAX_NUM_CANDIDATES);
    }

    private static float randomOversample() {
        return randomFloatBetween(RescoreVectorBuilder.MIN_OVERSAMPLE, 10.0f, true);
    }

    public void testDefaultBaselineIsBoundedAndNeverExact() {
        boolean sampling = randomBoolean();
        KnnEvalSettings disk = rescoreFor("bbq_disk").defaultBaseline(sampling);
        assertEquals(20.0f, disk.getVisitPercentage(), 0.0f);
        assertEquals(100.0f, disk.getRescoreOversample(), 0.0f);
        assertNull(disk.getNumCandidates());
        assertFalse(disk.isExact());

        int maxCandidates = sampling ? KnnEvalRescore.MAX_NUM_CANDIDATES - 1 : KnnEvalRescore.MAX_NUM_CANDIDATES;
        KnnEvalSettings hnsw = rescoreFor("hnsw").defaultBaseline(sampling);
        assertEquals(maxCandidates, hnsw.getNumCandidates().intValue());
        assertNull(hnsw.getRescoreOversample());
        assertNull(hnsw.getVisitPercentage());
        assertFalse(hnsw.isExact());

        KnnEvalSettings quantized = rescoreFor(randomFrom(QUANTIZED_GRAPH_TYPES)).defaultBaseline(sampling);
        assertEquals(maxCandidates, quantized.getNumCandidates().intValue());
        assertEquals(100.0f, quantized.getRescoreOversample(), 0.0f);
        assertNull(quantized.getVisitPercentage());
        assertFalse(quantized.isExact());
    }

    public void testRejectsKnobsOfTheOtherFamily() {
        String role = randomAlphaOfLength(8);
        String graphType = randomFrom(GRAPH_TYPES);
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> rescoreFor(graphType).validateSettings(new KnnEvalSettings(randomVisitPercentage(), null, null, false), role)
            ).getMessage(),
            allOf(
                containsString("[visit_percentage] applies only to bbq_disk, but the field is [" + graphType + "]"),
                containsString(", in the " + role + " ")
            )
        );
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> rescoreFor("bbq_disk").validateSettings(new KnnEvalSettings(null, randomNumCandidates(), null, false), role)
            ).getMessage(),
            containsString("[num_candidates] applies only to the hnsw family, but the field is [bbq_disk]; use [visit_percentage]")
        );
        KnnEvalSettings oversample = new KnnEvalSettings(null, randomNumCandidates(), randomOversample(), false);
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> rescoreFor("hnsw").validateSettings(oversample, role)).getMessage(),
            containsString("does not apply to an unquantized [hnsw] field")
        );
        rescoreFor(randomFrom(QUANTIZED_GRAPH_TYPES)).validateSettings(oversample, role);
    }

    public void testEveryIndexTypeAcceptsItsOwnKnobs() {
        String role = randomAlphaOfLength(8);
        rescoreFor("bbq_disk").validateSettings(
            new KnnEvalSettings(randomVisitPercentage(), null, randomBoolean() ? null : randomOversample(), false),
            role
        );
        String graphType = randomFrom(GRAPH_TYPES);
        Float oversample = graphType.equals("hnsw") || randomBoolean() ? null : randomOversample();
        rescoreFor(graphType).validateSettings(new KnnEvalSettings(null, randomNumCandidates(), oversample, false), role);
        rescoreFor(randomFrom(Stream.concat(Stream.of("bbq_disk"), GRAPH_TYPES.stream()).toList())).validateSettings(
            new KnnEvalSettings(null, null, null, true),
            role
        );
    }

    public void testAutoCalibrateAppliesOnlyToDiskBbq() {
        String indexType = randomFrom(Stream.concat(Stream.of("bbq_disk"), GRAPH_TYPES.stream()).toList());
        boolean autoCalibrate = randomBoolean();
        KnnEvalRescore rescore = KnnEvalRescore.fromFieldMapping(
            "emb",
            Map.of("type", "dense_vector", "index_options", Map.of("type", indexType, "auto_calibrate", autoCalibrate))
        );
        assertEquals(indexType.equals("bbq_disk") && autoCalibrate, rescore.autoCalibrate());
    }

    public void testQuantizedScoresFollowTheIndexTypeAndRescoring() {
        KnnEvalSettings noOversample = new KnnEvalSettings(null, randomNumCandidates(), null, false);
        assertFalse(withoutMappingOversample("hnsw").returnsQuantizedScores(noOversample));
        KnnEvalRescore quantized = withoutMappingOversample(randomFrom(QUANTIZED_GRAPH_TYPES));
        assertTrue(quantized.returnsQuantizedScores(noOversample));
        assertFalse(quantized.returnsQuantizedScores(new KnnEvalSettings(null, randomNumCandidates(), randomOversample(), false)));
        assertFalse(quantized.returnsQuantizedScores(new KnnEvalSettings(null, null, null, true)));
    }

    public void testDetectsCappedRescoreWindow() {
        KnnEvalRescore rescore = rescoreFor("bbq_disk");
        assertFalse(rescore.autoCalibrate());
        assertFalse(rescore.isRescoreWindowCapped(10, 10.0f));
        assertTrue(rescore.isRescoreWindowCapped(10, 10000.0f));
    }

    public void testScoresAreQuantizedOnlyWhenNothingRescores() {
        Map<String, Object> noRescore = Map.of("type", "dense_vector", "index_options", Map.of("type", "bbq_disk", "bits", 4));
        KnnEvalRescore unrescored = KnnEvalRescore.fromFieldMapping("emb", noRescore);
        assertTrue(unrescored.returnsQuantizedScores(new KnnEvalSettings(5.0f, null, null, false)));
        // a request or mapping oversample, or an exact search, gives full-precision scores
        assertFalse(unrescored.returnsQuantizedScores(new KnnEvalSettings(5.0f, null, 3.0f, false)));
        assertFalse(unrescored.returnsQuantizedScores(new KnnEvalSettings(null, null, null, true)));
        assertFalse(rescoreFor("bbq_disk").returnsQuantizedScores(new KnnEvalSettings(5.0f, null, null, false)));
    }

    public void testAutoCalibrationRescoresWithoutAnOversample() {
        KnnEvalRescore rescore = KnnEvalRescore.fromFieldMapping(
            "emb",
            Map.of("type", "dense_vector", "index_options", Map.of("type", "bbq_disk", "bits", 4, "auto_calibrate", true))
        );
        assertFalse(rescore.returnsQuantizedScores(new KnnEvalSettings(5.0f, null, null, false)));
    }

    public void testAutoCalibrationIsReadFromTheMapping() {
        KnnEvalRescore rescore = KnnEvalRescore.fromFieldMapping(
            "emb",
            Map.of(
                "type",
                "dense_vector",
                "index_options",
                Map.of("type", "bbq_disk", "auto_calibrate", true, "rescore_vector", Map.of("oversample", 3.0))
            )
        );
        assertTrue(rescore.autoCalibrate());
    }
}
