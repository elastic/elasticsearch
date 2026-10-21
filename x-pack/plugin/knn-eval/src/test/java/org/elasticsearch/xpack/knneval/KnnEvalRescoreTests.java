/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class KnnEvalRescoreTests extends ESTestCase {

    private static KnnEvalRescore rescoreFor(String indexType) {
        return KnnEvalRescore.fromFieldMapping(
            "emb",
            Map.of("type", "dense_vector", "index_options", Map.of("type", indexType, "rescore_vector", Map.of("oversample", 3.0)))
        );
    }

    public void testDiskBbqAndHnswFamilyAreSupported() {
        for (String indexType : List.of("bbq_disk", "hnsw", "int8_hnsw", "int4_hnsw", "bbq_hnsw")) {
            assertNotNull(rescoreFor(indexType));
        }
        for (String indexType : List.of("flat", "int8_flat", "int4_flat", "bbq_flat")) {
            assertThat(
                expectThrows(
                    IllegalArgumentException.class,
                    () -> KnnEvalRescore.fromFieldMapping("emb", Map.of("type", "dense_vector", "index_options", Map.of("type", indexType)))
                ).getMessage(),
                containsString(
                    "field [emb] must use [index_options.type] of one of [bbq_disk, hnsw, int8_hnsw, int4_hnsw, bbq_hnsw], found ["
                        + indexType
                        + "]"
                )
            );
        }
    }

    public void testExplicitIndexOptionsAreRequired() {
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> KnnEvalRescore.fromFieldMapping("emb", Map.of("type", "dense_vector")))
                .getMessage(),
            containsString("found no [index_options] in mapping [{type=dense_vector}]")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> KnnEvalRescore.fromFieldMapping("emb", null)).getMessage(),
            containsString("found no [index_options] in mapping [null]")
        );
    }

    public void testDefaultBaselineIsBoundedAndNeverExact() {
        KnnEvalSettings disk = rescoreFor("bbq_disk").defaultBaseline(false);
        assertEquals(20.0f, disk.getVisitPercentage(), 0.0f);
        assertEquals(100.0f, disk.getRescoreOversample(), 0.0f);
        assertNull(disk.getNumCandidates());

        KnnEvalSettings hnsw = rescoreFor("hnsw").defaultBaseline(false);
        assertEquals(KnnEvalRescore.MAX_NUM_CANDIDATES, hnsw.getNumCandidates().intValue());
        assertNull(hnsw.getRescoreOversample());
        assertNull(hnsw.getVisitPercentage());
        // a sampled query also retrieves its own document
        assertEquals(KnnEvalRescore.MAX_NUM_CANDIDATES - 1, rescoreFor("hnsw").defaultBaseline(true).getNumCandidates().intValue());

        for (String indexType : List.of("int8_hnsw", "int4_hnsw", "bbq_hnsw")) {
            KnnEvalSettings quantized = rescoreFor(indexType).defaultBaseline(false);
            assertEquals(KnnEvalRescore.MAX_NUM_CANDIDATES, quantized.getNumCandidates().intValue());
            assertEquals(100.0f, quantized.getRescoreOversample(), 0.0f);
            assertFalse(quantized.isExact());
        }
    }

    public void testGraphFieldsRejectVisitPercentage() {
        KnnEvalSettings visit = new KnnEvalSettings(5.0f, null, null, false);
        for (String indexType : List.of("hnsw", "int8_hnsw", "int4_hnsw", "bbq_hnsw")) {
            assertThat(
                expectThrows(IllegalArgumentException.class, () -> rescoreFor(indexType).validateSettings(visit, "baseline")).getMessage(),
                containsString("[visit_percentage] applies only to bbq_disk, but the field is [" + indexType + "]")
            );
        }
        rescoreFor("bbq_disk").validateSettings(visit, "baseline");
    }

    public void testBbqDiskRejectsNumCandidates() {
        KnnEvalSettings candidates = new KnnEvalSettings(null, 100, null, false);
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> rescoreFor("bbq_disk").validateSettings(candidates, "baseline"))
                .getMessage(),
            containsString("[num_candidates] applies only to the hnsw family, but the field is [bbq_disk]; use [visit_percentage]")
        );
    }

    public void testEveryIndexTypeAcceptsItsOwnKnobs() {
        rescoreFor("bbq_disk").validateSettings(new KnnEvalSettings(5.0f, null, 3.0f, false), "baseline");
        rescoreFor("bbq_disk").validateSettings(new KnnEvalSettings(null, null, null, true), "baseline");
        for (String indexType : List.of("hnsw", "int8_hnsw", "int4_hnsw", "bbq_hnsw")) {
            rescoreFor(indexType).validateSettings(new KnnEvalSettings(null, 100, null, false), "baseline");
            rescoreFor(indexType).validateSettings(new KnnEvalSettings(null, null, null, true), "baseline");
        }
    }

    public void testUnquantizedHnswRejectsRescoreOversample() {
        KnnEvalSettings oversample = new KnnEvalSettings(null, 100, 3.0f, false);
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> rescoreFor("hnsw").validateSettings(oversample, "knn_settings entry"))
                .getMessage(),
            containsString("does not apply to an unquantized [hnsw] field")
        );
        rescoreFor("int8_hnsw").validateSettings(oversample, "knn_settings entry");
    }

    public void testUnquantizedHnswScoresAreNeverQuantized() {
        KnnEvalRescore hnsw = KnnEvalRescore.fromFieldMapping(
            "emb",
            Map.of("type", "dense_vector", "index_options", Map.of("type", "hnsw"))
        );
        assertFalse(hnsw.returnsQuantizedScores(new KnnEvalSettings(null, 100, null, false)));
        KnnEvalRescore bbq = KnnEvalRescore.fromFieldMapping(
            "emb",
            Map.of("type", "dense_vector", "index_options", Map.of("type", "bbq_hnsw"))
        );
        assertTrue(bbq.returnsQuantizedScores(new KnnEvalSettings(null, 100, null, false)));
        assertFalse(bbq.returnsQuantizedScores(new KnnEvalSettings(null, 100, 3.0f, false)));
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
