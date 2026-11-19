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

public class KnnEvalFieldTypeTests extends ESTestCase {

    private static KnnEvalFieldType fieldTypeFor(String indexType) {
        return KnnEvalFieldType.fromFieldMapping(
            "emb",
            Map.of("type", "dense_vector", "index_options", Map.of("type", indexType, "rescore_vector", Map.of("oversample", 3.0)))
        );
    }

    /** Unlike {@link #fieldTypeFor}, the mapping has no oversample, so nothing rescores unless the request says so. */
    private static KnnEvalFieldType withoutMappingOversample(String indexType) {
        return KnnEvalFieldType.fromFieldMapping("emb", Map.of("type", "dense_vector", "index_options", Map.of("type", indexType)));
    }

    public void testDiskBbqAndHnswFamilyAreSupported() {
        for (String indexType : List.of("bbq_disk", "hnsw", "int8_hnsw", "int4_hnsw", "bbq_hnsw")) {
            assertNotNull(fieldTypeFor(indexType));
        }
        // flat types scan every vector, so there is no search effort to evaluate; unknown types are reported alike
        for (String indexType : List.of("flat", "int8_flat", "int4_flat", "bbq_flat", "made_up")) {
            assertThat(
                expectThrows(
                    IllegalArgumentException.class,
                    () -> KnnEvalFieldType.fromFieldMapping(
                        "emb",
                        Map.of("type", "dense_vector", "index_options", Map.of("type", indexType))
                    )
                ).getMessage(),
                containsString(
                    "field [emb] must use [index_options.type] of [hnsw, int8_hnsw, int4_hnsw, bbq_hnsw, bbq_disk], found ["
                        + indexType
                        + "]"
                )
            );
        }
    }

    public void testExplicitIndexOptionsAreRequired() {
        for (Map<String, Object> mapping : Arrays.<Map<String, Object>>asList(Map.of("type", "dense_vector"), null)) {
            assertThat(
                expectThrows(IllegalArgumentException.class, () -> KnnEvalFieldType.fromFieldMapping("emb", mapping)).getMessage(),
                allOf(
                    containsString("field [emb] must use [index_options.type] of [hnsw"),
                    containsString("found no [index_options] in mapping [")
                )
            );
        }
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> KnnEvalFieldType.fromFieldMapping("emb", Map.of("type", "dense_vector", "index_options", Map.of()))
            ).getMessage(),
            containsString("found no [index_options.type]")
        );
    }

    private static final List<String> GRAPH_TYPES = List.of("hnsw", "int8_hnsw", "int4_hnsw", "bbq_hnsw");
    private static final List<String> QUANTIZED_GRAPH_TYPES = List.of("int8_hnsw", "int4_hnsw", "bbq_hnsw");

    private static float randomVisitPercentage() {
        return randomFloatBetween(0.0f, 100.0f, true);
    }

    private static int randomNumCandidates() {
        return randomIntBetween(1, KnnEvalFieldType.MAX_NUM_CANDIDATES);
    }

    private static float randomOversample() {
        return randomFloatBetween(RescoreVectorBuilder.MIN_OVERSAMPLE, 10.0f, true);
    }

    public void testDefaultBaselineIsBoundedAndNeverExact() {
        boolean sampling = randomBoolean();
        int k = randomIntBetween(1, KnnEvalSpec.MAX_K);
        int limit = sampling ? KnnEvalFieldType.MAX_NUM_CANDIDATES - 1 : KnnEvalFieldType.MAX_NUM_CANDIDATES;
        int expectedNumCandidates = Math.clamp(100L * k, 1000, limit);
        KnnEvalSettings disk = fieldTypeFor("bbq_disk").defaultBaseline(k, sampling);
        assertEquals(20.0f, disk.getVisitPercentage(), 0.0f);
        assertEquals(100.0f, disk.getRescoreOversample(), 0.0f);
        assertNull(disk.getNumCandidates());
        assertFalse(disk.isExact());

        KnnEvalSettings hnsw = fieldTypeFor("hnsw").defaultBaseline(k, sampling);
        assertEquals(expectedNumCandidates, hnsw.getNumCandidates().intValue());
        assertNull(hnsw.getRescoreOversample());
        assertNull(hnsw.getVisitPercentage());
        assertFalse(hnsw.isExact());

        KnnEvalSettings quantized = fieldTypeFor(randomFrom(QUANTIZED_GRAPH_TYPES)).defaultBaseline(k, sampling);
        assertEquals(expectedNumCandidates, quantized.getNumCandidates().intValue());
        // the beam and the rescore window are the same depth between the floor and the limit
        assertEquals(1000, fieldTypeFor("hnsw").defaultBaseline(10, false).getNumCandidates().intValue());
        assertEquals(5000, fieldTypeFor("hnsw").defaultBaseline(50, false).getNumCandidates().intValue());
        assertEquals(10_000, fieldTypeFor("hnsw").defaultBaseline(100, false).getNumCandidates().intValue());
        assertEquals(9999, fieldTypeFor("hnsw").defaultBaseline(100, true).getNumCandidates().intValue());
        assertEquals(10_000, fieldTypeFor("hnsw").defaultBaseline(KnnEvalSpec.MAX_K, false).getNumCandidates().intValue());
        assertEquals(100.0f, quantized.getRescoreOversample(), 0.0f);
        assertNull(quantized.getVisitPercentage());
        assertFalse(quantized.isExact());
    }

    public void testRejectsSettingsOfTheOtherFamily() {
        String role = randomAlphaOfLength(8);
        String graphType = randomFrom(GRAPH_TYPES);
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> fieldTypeFor(graphType).validateSettings(new KnnEvalSettings(randomVisitPercentage(), null, null, false), role)
            ).getMessage(),
            allOf(
                containsString("[visit_percentage] applies only to bbq_disk, but the field is [" + graphType + "]"),
                containsString(", in the " + role + " ")
            )
        );
        fieldTypeFor("bbq_disk").validateSettings(new KnnEvalSettings(null, randomNumCandidates(), null, false), role);
        KnnEvalSettings oversample = new KnnEvalSettings(null, randomNumCandidates(), randomOversample(), false);
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> fieldTypeFor("hnsw").validateSettings(oversample, role)).getMessage(),
            containsString("does not apply to an unquantized [hnsw] field")
        );
        fieldTypeFor(randomFrom(QUANTIZED_GRAPH_TYPES)).validateSettings(oversample, role);
    }

    public void testRejectsNumCandidatesWithAFixedVisitOnDisk() {
        String role = randomAlphaOfLength(8);
        KnnEvalFieldType disk = fieldTypeFor("bbq_disk");
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> disk.validateSettings(
                    new KnnEvalSettings(randomFloatBetween(1.0f, 100.0f, true), randomNumCandidates(), null, false),
                    role
                )
            ).getMessage(),
            containsString("[num_candidates] has no effect on bbq_disk with a positive [visit_percentage]")
        );
        // visit_percentage 0 lets DiskBBQ pick the visit per query, so num_candidates still applies
        disk.validateSettings(new KnnEvalSettings(0.0f, randomNumCandidates(), null, false), role);
    }

    public void testEveryIndexTypeAcceptsItsOwnSettings() {
        String role = randomAlphaOfLength(8);
        fieldTypeFor("bbq_disk").validateSettings(
            new KnnEvalSettings(randomVisitPercentage(), null, randomBoolean() ? null : randomOversample(), false),
            role
        );
        String graphType = randomFrom(GRAPH_TYPES);
        Float oversample = graphType.equals("hnsw") || randomBoolean() ? null : randomOversample();
        fieldTypeFor(graphType).validateSettings(new KnnEvalSettings(null, randomNumCandidates(), oversample, false), role);
        fieldTypeFor(randomFrom(Stream.concat(Stream.of("bbq_disk"), GRAPH_TYPES.stream()).toList())).validateSettings(
            new KnnEvalSettings(null, null, null, true),
            role
        );
    }

    public void testRejectsADiskCandidateDeeperThanTheBaseline() {
        KnnEvalFieldType disk = fieldTypeFor("bbq_disk");
        float baselineVisit = randomFloatBetween(1.0f, 99.0f, true);
        KnnEvalSettings baseline = new KnnEvalSettings(baselineVisit, null, null, false);
        disk.validateDepth(baseline, new KnnEvalSettings(baselineVisit, null, null, false), 10);
        disk.validateDepth(baseline, new KnnEvalSettings(randomFloatBetween(0.0f, baselineVisit, true), null, null, false), 10);
        KnnEvalSettings deeper = visit(randomFloatBetween(Math.nextUp(baselineVisit), 100.0f, true));
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> disk.validateDepth(baseline, deeper, 10)).getMessage(),
            containsString("searches deeper than the baseline")
        );
        // the default baseline is checked too
        expectThrows(IllegalArgumentException.class, () -> disk.validateDepth(disk.defaultBaseline(10, false), visit(20.5f), 10));
        // depth picked per query can't be compared
        disk.validateDepth(baseline, new KnnEvalSettings(null, randomNumCandidates(), null, false), 10);
        disk.validateDepth(new KnnEvalSettings(null, randomNumCandidates(), null, false), deeper, 10);
        disk.validateDepth(visit(0.0f), deeper, 10);
        disk.validateDepth(new KnnEvalSettings(null, null, null, true), visit(100.0f), 10);
    }

    private static KnnEvalSettings visit(float visitPercentage) {
        return new KnnEvalSettings(visitPercentage, null, null, false);
    }

    public void testRejectsAGraphCandidateWithAWiderBeamThanTheBaseline() {
        KnnEvalFieldType graph = withoutMappingOversample(randomFrom(GRAPH_TYPES));
        int k = 10;
        KnnEvalSettings baseline = new KnnEvalSettings(null, 1000, null, false);
        graph.validateDepth(baseline, new KnnEvalSettings(null, randomIntBetween(k, 1000), null, false), k);
        // an unset num_candidates defaults to 1.5 * k
        graph.validateDepth(baseline, new KnnEvalSettings(null, null, null, false), k);
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> graph.validateDepth(baseline, new KnnEvalSettings(null, randomIntBetween(1001, 10_000), null, false), k)
            ).getMessage(),
            containsString("searches deeper than the baseline")
        );
        // the default baseline is checked too: 100 * k, with the quantized rescore window never widening past it
        KnnEvalSettings defaultBaseline = graph.defaultBaseline(k, false);
        graph.validateDepth(defaultBaseline, new KnnEvalSettings(null, 1000, null, false), k);
        graph.validateDepth(defaultBaseline, new KnnEvalSettings(null, null, 100.0f, false), k);
        expectThrows(
            IllegalArgumentException.class,
            () -> graph.validateDepth(defaultBaseline, new KnnEvalSettings(null, 1001, null, false), k)
        );
        KnnEvalSettings wideDefault = graph.defaultBaseline(50, false);
        graph.validateDepth(wideDefault, new KnnEvalSettings(null, 5000, null, false), 50);
        expectThrows(
            IllegalArgumentException.class,
            () -> graph.validateDepth(wideDefault, new KnnEvalSettings(null, 5001, null, false), 50)
        );
        graph.validateDepth(new KnnEvalSettings(null, null, null, true), new KnnEvalSettings(null, 10_000, null, false), k);
    }

    public void testRescoringWidensAQuantizedGraphBeam() {
        int k = 10;
        KnnEvalSettings baseline = new KnnEvalSettings(null, 100, null, false);
        // k * oversample = 200 > num_candidates
        KnnEvalSettings oversampled = new KnnEvalSettings(null, 100, 20.0f, false);
        expectThrows(
            IllegalArgumentException.class,
            () -> withoutMappingOversample(randomFrom(QUANTIZED_GRAPH_TYPES)).validateDepth(baseline, oversampled, k)
        );
        // the mapping's oversample applies to both, so neither is wider
        KnnEvalFieldType mapped = KnnEvalFieldType.fromFieldMapping(
            "emb",
            Map.of(
                "type",
                "dense_vector",
                "index_options",
                Map.of("type", randomFrom(QUANTIZED_GRAPH_TYPES), "rescore_vector", Map.of("oversample", 20.0))
            )
        );
        mapped.validateDepth(baseline, new KnnEvalSettings(null, 100, null, false), k);
        // an unquantized graph never rescores
        withoutMappingOversample("hnsw").validateDepth(baseline, new KnnEvalSettings(null, 100, null, false), k);
    }

    public void testAutoCalibrateAppliesOnlyToDiskBbq() {
        String indexType = randomFrom(Stream.concat(Stream.of("bbq_disk"), GRAPH_TYPES.stream()).toList());
        boolean autoCalibrate = randomBoolean();
        KnnEvalFieldType fieldType = KnnEvalFieldType.fromFieldMapping(
            "emb",
            Map.of("type", "dense_vector", "index_options", Map.of("type", indexType, "auto_calibrate", autoCalibrate))
        );
        assertEquals(indexType.equals("bbq_disk") && autoCalibrate, fieldType.autoCalibrate());
    }

    public void testQuantizedScoresFollowTheIndexTypeAndRescoring() {
        KnnEvalSettings noOversample = new KnnEvalSettings(null, randomNumCandidates(), null, false);
        assertFalse(withoutMappingOversample("hnsw").returnsQuantizedScores(noOversample));
        KnnEvalFieldType quantized = withoutMappingOversample(randomFrom(QUANTIZED_GRAPH_TYPES));
        assertTrue(quantized.returnsQuantizedScores(noOversample));
        assertFalse(quantized.returnsQuantizedScores(new KnnEvalSettings(null, randomNumCandidates(), randomOversample(), false)));
        assertFalse(quantized.returnsQuantizedScores(new KnnEvalSettings(null, null, null, true)));
    }

    public void testDetectsCappedRescoreWindow() {
        KnnEvalFieldType fieldType = fieldTypeFor("bbq_disk");
        assertFalse(fieldType.autoCalibrate());
        assertFalse(fieldType.isRescoreWindowCapped(10, 10.0f));
        assertTrue(fieldType.isRescoreWindowCapped(10, 10000.0f));
    }

    public void testScoresAreQuantizedOnlyWhenNothingRescores() {
        Map<String, Object> noRescore = Map.of("type", "dense_vector", "index_options", Map.of("type", "bbq_disk", "bits", 4));
        KnnEvalFieldType unrescored = KnnEvalFieldType.fromFieldMapping("emb", noRescore);
        assertTrue(unrescored.returnsQuantizedScores(new KnnEvalSettings(5.0f, null, null, false)));
        // a request or mapping oversample, or an exact search, gives full-precision scores
        assertFalse(unrescored.returnsQuantizedScores(new KnnEvalSettings(5.0f, null, 3.0f, false)));
        assertFalse(unrescored.returnsQuantizedScores(new KnnEvalSettings(null, null, null, true)));
        assertFalse(fieldTypeFor("bbq_disk").returnsQuantizedScores(new KnnEvalSettings(5.0f, null, null, false)));
    }

    public void testAutoCalibrationRescoresWithoutAnOversample() {
        KnnEvalFieldType fieldType = KnnEvalFieldType.fromFieldMapping(
            "emb",
            Map.of("type", "dense_vector", "index_options", Map.of("type", "bbq_disk", "bits", 4, "auto_calibrate", true))
        );
        assertFalse(fieldType.returnsQuantizedScores(new KnnEvalSettings(5.0f, null, null, false)));
    }

    public void testAutoCalibrationIsReadFromTheMapping() {
        KnnEvalFieldType fieldType = KnnEvalFieldType.fromFieldMapping(
            "emb",
            Map.of(
                "type",
                "dense_vector",
                "index_options",
                Map.of("type", "bbq_disk", "auto_calibrate", true, "rescore_vector", Map.of("oversample", 3.0))
            )
        );
        assertTrue(fieldType.autoCalibrate());
    }
}
