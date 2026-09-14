/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.knneval;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * The knob compatibility matrix. Elasticsearch accepts a knob its index type cannot use and then ignores it, so a sweep over such a
 * knob would report a flat recall and read as good news; these are the checks that turn that into an error instead.
 */
public class KnnEvalRescoreTests extends ESTestCase {

    private static final List<String> IVF_TYPES = List.of("bbq_disk");
    private static final List<String> GRAPH_AND_FLAT_TYPES = List.of(
        "hnsw",
        "int8_hnsw",
        "int4_hnsw",
        "bbq_hnsw",
        "flat",
        "int8_flat",
        "int4_flat",
        "bbq_flat"
    );
    private static final List<String> UNQUANTIZED_TYPES = List.of("hnsw", "flat");

    private static KnnEvalRescore rescoreFor(String indexType) {
        return KnnEvalRescore.fromFieldMapping(Map.of("type", "dense_vector", "index_options", Map.of("type", indexType)));
    }

    public void testVisitPercentageIsOnlyForTheIvfFamily() {
        for (String indexType : IVF_TYPES) {
            rescoreFor(indexType).validateSupportedKnobs(new KnnEvalKnobs(50.0f, null, null, false));
        }
        for (String indexType : GRAPH_AND_FLAT_TYPES) {
            // 0 means "let the index choose" rather than "unset", so it is rejected like any other value
            for (float visitPercentage : new float[] { 0.0f, 50.0f }) {
                assertThat(
                    indexType,
                    expectThrows(
                        IllegalArgumentException.class,
                        () -> rescoreFor(indexType).validateSupportedKnobs(new KnnEvalKnobs(visitPercentage, null, null, false))
                    ).getMessage(),
                    containsString("[visit_percentage] is not supported for index_options type [" + indexType + "]; use [num_candidates]")
                );
            }
        }
    }

    public void testOversampleIsOnlyForQuantizedTypes() {
        for (String indexType : UNQUANTIZED_TYPES) {
            assertThat(
                indexType,
                expectThrows(
                    IllegalArgumentException.class,
                    () -> rescoreFor(indexType).validateSupportedKnobs(new KnnEvalKnobs(null, null, 3.0f, false))
                ).getMessage(),
                containsString("[oversample] has no effect on unquantized index_options type [" + indexType + "]")
            );
        }
        for (String indexType : List.of("bbq_disk", "int8_hnsw", "int4_hnsw", "bbq_hnsw", "int8_flat", "int4_flat", "bbq_flat")) {
            rescoreFor(indexType).validateSupportedKnobs(new KnnEvalKnobs(null, null, 3.0f, false));
        }
    }

    public void testNumCandidatesAppliesEverywhere() {
        for (String indexType : IVF_TYPES) {
            rescoreFor(indexType).validateSupportedKnobs(new KnnEvalKnobs(null, 100, null, false));
        }
        for (String indexType : GRAPH_AND_FLAT_TYPES) {
            rescoreFor(indexType).validateSupportedKnobs(new KnnEvalKnobs(null, 100, null, false));
        }
    }

    public void testNothingIsValidatedWithoutAKnownType() {
        // an exact run configures nothing approximate, and an unknown or absent type is no basis for rejecting anything
        rescoreFor("hnsw").validateSupportedKnobs(new KnnEvalKnobs(null, null, null, true));
        KnnEvalRescore.fromFieldMapping(Map.of("type", "dense_vector")).validateSupportedKnobs(new KnnEvalKnobs(50.0f, null, 3.0f, false));
        KnnEvalRescore.fromFieldMapping(Map.of("type", "dense_vector", "index_options", Map.of("type", "not_a_type")))
            .validateSupportedKnobs(new KnnEvalKnobs(50.0f, null, 3.0f, false));
    }

    public void testUnquantizedTypesRescoreNothing() {
        for (String indexType : UNQUANTIZED_TYPES) {
            KnnEvalRescore rescore = rescoreFor(indexType);
            assertFalse(rescore.quantized());
            assertEquals(0, rescore.rescoreWindow(10, null));
            // with no rescore window to floor it, the candidate window is just the default 1.5 x k
            assertEquals(15, rescore.effectiveNumCandidates(10, null, null));
            assertEquals(100, rescore.effectiveNumCandidates(10, 100, null));
        }
    }

    public void testQuantizedTypesWidenTheWindowToTheRescorePass() {
        KnnEvalRescore rescore = KnnEvalRescore.fromFieldMapping(
            Map.of("type", "dense_vector", "index_options", Map.of("type", "int8_hnsw", "rescore_vector", Map.of("oversample", 3.0)))
        );
        assertTrue(rescore.quantized());
        assertEquals(30, rescore.rescoreWindow(10, null));
        // the rescore window floors num_candidates, which is what the mapper does
        assertEquals(30, rescore.effectiveNumCandidates(10, null, null));
        assertEquals(50, rescore.effectiveNumCandidates(10, 50, null));
        // and a knob oversample overrides the mapping's
        assertEquals(100, rescore.rescoreWindow(10, 10.0f));
        assertEquals(100, rescore.effectiveNumCandidates(10, 50, 10.0f));
        // both are capped
        assertEquals(10000, rescore.rescoreWindow(10, 10000.0f));
        assertEquals(10000, rescore.effectiveNumCandidates(10, 50, 10000.0f));
    }
}
