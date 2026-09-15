/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.knneval;

import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorSimilarity;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class KnnEvalFidelityTests extends ESTestCase {

    /**
     * The float branches of {@code VectorSimilarity#score}, replicated because that method is package-private. This copy is what
     * catches drift between it and {@link KnnEvalFidelity#invert}.
     */
    private static float score(VectorSimilarity similarity, double value) {
        return switch (similarity) {
            case L2_NORM -> (float) (1.0 / (1.0 + value * value));
            case COSINE, DOT_PRODUCT -> (float) ((1.0 + value) / 2.0);
            case MAX_INNER_PRODUCT -> (float) (value < 0 ? 1.0 / (1.0 - value) : value + 1.0);
        };
    }

    public void testInversionRoundTripsTheScoreTransform() {
        for (VectorSimilarity similarity : VectorSimilarity.values()) {
            KnnEvalFidelity fidelity = new KnnEvalFidelity(similarity, null);
            for (int i = 0; i < 100; i++) {
                // l2_norm inverts to a distance, which is non-negative; a similarity is not
                double value = similarity == VectorSimilarity.L2_NORM
                    ? randomDoubleBetween(0.0, 20.0, true)
                    : randomDoubleBetween(-0.99, 0.99, true);
                double inverted = fidelity.invert(score(similarity, value));
                assertEquals(similarity + " did not round trip for " + value, value, inverted, 1e-3);
            }
        }
    }

    public void testMaxInnerProductInvertsBothBranches() {
        KnnEvalFidelity fidelity = new KnnEvalFidelity(VectorSimilarity.MAX_INNER_PRODUCT, null);
        // the transform is piecewise around a similarity of 0, which maps to a score of exactly 1
        assertEquals(0.0, fidelity.invert(1.0f), 1e-6);
        assertEquals(4.0, fidelity.invert(5.0f), 1e-6);
        assertEquals(-1.0, fidelity.invert(0.5f), 1e-6);
    }

    public void testEpsilonIsZeroForIdenticalScores() {
        for (VectorSimilarity similarity : VectorSimilarity.values()) {
            KnnEvalFidelity fidelity = new KnnEvalFidelity(similarity, null);
            float score = score(similarity, similarity == VectorSimilarity.L2_NORM ? 2.0 : 0.5);
            assertEquals(similarity.toString(), 0.0, fidelity.epsilonAtRank(score, score), 0.0);
        }
    }

    public void testEpsilonGrowsAsTheCandidateGetsWorse() {
        KnnEvalFidelity cosine = new KnnEvalFidelity(VectorSimilarity.COSINE, null);
        assertEquals(1.0, cosine.epsilonAtRank(score(VectorSimilarity.COSINE, 0.9), score(VectorSimilarity.COSINE, 0.45)), 1e-6);

        KnnEvalFidelity l2 = new KnnEvalFidelity(VectorSimilarity.L2_NORM, null);
        // the ratio flips for a distance
        assertEquals(1.0, l2.epsilonAtRank(score(VectorSimilarity.L2_NORM, 1.0), score(VectorSimilarity.L2_NORM, 2.0)), 1e-6);
        // closer than the reference is not a loss, so epsilon floors at zero
        assertEquals(0.0, l2.epsilonAtRank(score(VectorSimilarity.L2_NORM, 2.0), score(VectorSimilarity.L2_NORM, 1.0)), 0.0);
    }

    public void testEpsilonIsUnboundedWhenNoRatioExists() {
        KnnEvalFidelity cosine = new KnnEvalFidelity(VectorSimilarity.COSINE, null);
        // a non-positive candidate similarity has no ratio against a positive reference
        assertNull(cosine.epsilonAtRank(score(VectorSimilarity.COSINE, 0.9), score(VectorSimilarity.COSINE, 0.0)));
        assertNull(cosine.epsilonAtRank(score(VectorSimilarity.COSINE, 0.9), score(VectorSimilarity.COSINE, -0.5)));

        KnnEvalFidelity l2 = new KnnEvalFidelity(VectorSimilarity.L2_NORM, null);
        // against an exact match, anything further away is unboundedly worse
        assertNull(l2.epsilonAtRank(score(VectorSimilarity.L2_NORM, 0.0), score(VectorSimilarity.L2_NORM, 1.0)));
        assertEquals(0.0, l2.epsilonAtRank(score(VectorSimilarity.L2_NORM, 0.0), score(VectorSimilarity.L2_NORM, 0.0)), 0.0);
    }

    /** Derives both halves from one mapping, as the transport action does. */
    private static KnnEvalFidelity fidelityOf(Map<String, Object> fieldMapping, Float baselineOversample) {
        return KnnEvalFidelity.fromFieldMapping("emb", fieldMapping, KnnEvalRescore.fromFieldMapping(fieldMapping), baselineOversample);
    }

    private static Map<String, Object> denseVector(String indexType, Object oversample) {
        Map<String, Object> indexOptions = oversample == null
            ? Map.of("type", indexType)
            : Map.of("type", indexType, "rescore_vector", Map.of("oversample", oversample));
        return Map.of("type", "dense_vector", "similarity", "l2_norm", "element_type", "float", "index_options", indexOptions);
    }

    public void testMappingIsReadForSimilarityAndElementType() {
        KnnEvalFidelity fidelity = fidelityOf(denseVector("bbq_disk", 3.0), null);
        assertFalse(fidelity.isSkipped());
        assertTrue(fidelity.isDistanceBased());
        assertEquals(VectorSimilarity.L2_NORM, fidelity.similarity());

        // defaults to the mapper's own default when a mapping omits it
        assertEquals(VectorSimilarity.COSINE, fidelityOf(Map.of("type", "dense_vector"), null).similarity());
    }

    public void testFidelityIsSkippedWhenScoresAreEstimates() {
        KnnEvalFidelity fidelity = fidelityOf(denseVector("bbq_disk", 0), null);
        assertTrue(fidelity.isSkipped());
        assertEquals(KnnEvalFidelity.RESCORING_DISABLED, fidelity.skippedReason());

        // ... unless the baseline asks for rescoring itself
        assertFalse(fidelityOf(denseVector("bbq_disk", 0), 10.0f).isSkipped());
        assertFalse(fidelityOf(denseVector("bbq_disk", 3.0), null).isSkipped());
    }

    /** An unquantized index has nothing to rescore because its scores were never estimates. */
    public void testFidelityIsNotSkippedForUnquantizedTypes() {
        for (String indexType : new String[] { "hnsw", "flat" }) {
            assertFalse(indexType, fidelityOf(denseVector(indexType, null), null).isSkipped());
        }
        // whereas a quantized type with no rescore_vector is estimating
        for (String indexType : new String[] { "int8_hnsw", "int4_flat", "bbq_hnsw" }) {
            assertTrue(indexType, fidelityOf(denseVector(indexType, null), null).isSkipped());
        }
    }

    public void testFidelityIsSkippedForNonFloatElementTypes() {
        KnnEvalFidelity fidelity = KnnEvalFidelity.fromFieldMapping(
            "emb",
            Map.of("type", "dense_vector", "similarity", "l2_norm", "element_type", "byte"),
            null,
            null
        );
        assertTrue(fidelity.isSkipped());
        assertThat(fidelity.skippedReason(), containsString("only defined for float element types"));
    }

    public void testNonVectorFieldsAreRejected() {
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> KnnEvalFidelity.fromFieldMapping("emb", Map.of("type", "keyword"), null, null)
            ).getMessage(),
            containsString("field [emb] is of type [keyword], but [dense_vector] is required")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> KnnEvalFidelity.fromFieldMapping("emb", null, null, null)).getMessage(),
            containsString("field [emb] is not mapped in any of the requested indices")
        );
    }
}
