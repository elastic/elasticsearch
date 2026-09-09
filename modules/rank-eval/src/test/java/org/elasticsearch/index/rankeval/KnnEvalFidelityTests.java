/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorSimilarity;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class KnnEvalFidelityTests extends ESTestCase {

    /**
     * The forward transform, replicated here because {@code VectorSimilarity#score} is package-private to
     * {@code org.elasticsearch.index.mapper.vectors}. These are the float-element-type branches of that method; if it ever changes,
     * {@link KnnEvalFidelity#invert} has to change with it and this copy is what will catch the drift.
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
                // l2_norm inverts to a distance, which is non-negative; the others invert to a similarity, which is not
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
        // baseline similarity 0.9, candidate 0.45: the candidate gave up half the similarity
        assertEquals(1.0, cosine.epsilonAtRank(score(VectorSimilarity.COSINE, 0.9), score(VectorSimilarity.COSINE, 0.45)), 1e-6);

        KnnEvalFidelity l2 = new KnnEvalFidelity(VectorSimilarity.L2_NORM, null);
        // the ratio flips for a distance: the candidate is twice as far away
        assertEquals(1.0, l2.epsilonAtRank(score(VectorSimilarity.L2_NORM, 1.0), score(VectorSimilarity.L2_NORM, 2.0)), 1e-6);
        // a candidate closer than the reference is not a loss, so epsilon floors at zero rather than going negative
        assertEquals(0.0, l2.epsilonAtRank(score(VectorSimilarity.L2_NORM, 2.0), score(VectorSimilarity.L2_NORM, 1.0)), 0.0);
    }

    public void testEpsilonIsUnboundedWhenNoRatioExists() {
        KnnEvalFidelity cosine = new KnnEvalFidelity(VectorSimilarity.COSINE, null);
        // a non-positive candidate similarity has no meaningful ratio against a positive reference
        assertNull(cosine.epsilonAtRank(score(VectorSimilarity.COSINE, 0.9), score(VectorSimilarity.COSINE, 0.0)));
        assertNull(cosine.epsilonAtRank(score(VectorSimilarity.COSINE, 0.9), score(VectorSimilarity.COSINE, -0.5)));

        KnnEvalFidelity l2 = new KnnEvalFidelity(VectorSimilarity.L2_NORM, null);
        // the reference found an exact match; anything further away is unboundedly worse, an exact match is not
        assertNull(l2.epsilonAtRank(score(VectorSimilarity.L2_NORM, 0.0), score(VectorSimilarity.L2_NORM, 1.0)));
        assertEquals(0.0, l2.epsilonAtRank(score(VectorSimilarity.L2_NORM, 0.0), score(VectorSimilarity.L2_NORM, 0.0)), 0.0);
    }

    public void testMappingIsReadForSimilarityAndElementType() {
        KnnEvalFidelity fidelity = KnnEvalFidelity.fromFieldMapping(
            "emb",
            Map.of("type", "dense_vector", "similarity", "l2_norm", "element_type", "float"),
            null
        );
        assertFalse(fidelity.isSkipped());
        assertTrue(fidelity.isDistanceBased());
        assertEquals(VectorSimilarity.L2_NORM, fidelity.similarity());

        // similarity defaults to the mapper's own default when a mapping omits it
        assertEquals(VectorSimilarity.COSINE, KnnEvalFidelity.fromFieldMapping("emb", Map.of("type", "dense_vector"), null).similarity());
    }

    public void testFidelityIsSkippedWhenScoresAreEstimates() {
        KnnEvalFidelity fidelity = KnnEvalFidelity.fromFieldMapping(
            "emb",
            Map.of(
                "type",
                "dense_vector",
                "similarity",
                "dot_product",
                "index_options",
                Map.of("type", "bbq_disk", "rescore_vector", Map.of("oversample", 0))
            ),
            null
        );
        assertTrue(fidelity.isSkipped());
        assertEquals(KnnEvalFidelity.RESCORING_DISABLED, fidelity.skippedReason());

        // ... unless the baseline run asks for rescoring itself, in which case its scores are real again
        assertFalse(
            KnnEvalFidelity.fromFieldMapping(
                "emb",
                Map.of("type", "dense_vector", "index_options", Map.of("type", "bbq_disk", "rescore_vector", Map.of("oversample", 0))),
                10.0f
            ).isSkipped()
        );

        // a non-zero oversample means the scores were recomputed on the real vectors, so the metric is meaningful
        assertFalse(
            KnnEvalFidelity.fromFieldMapping(
                "emb",
                Map.of("type", "dense_vector", "index_options", Map.of("type", "bbq_disk", "rescore_vector", Map.of("oversample", 3.0))),
                null
            ).isSkipped()
        );
    }

    public void testFidelityIsSkippedForNonFloatElementTypes() {
        KnnEvalFidelity fidelity = KnnEvalFidelity.fromFieldMapping(
            "emb",
            Map.of("type", "dense_vector", "similarity", "l2_norm", "element_type", "byte"),
            null
        );
        assertTrue(fidelity.isSkipped());
        assertThat(fidelity.skippedReason(), containsString("only defined for float element types"));
    }

    public void testNonVectorFieldsAreRejected() {
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> KnnEvalFidelity.fromFieldMapping("emb", Map.of("type", "keyword"), null))
                .getMessage(),
            containsString("field [emb] is of type [keyword], but [dense_vector] is required")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> KnnEvalFidelity.fromFieldMapping("emb", null, null)).getMessage(),
            containsString("field [emb] is not mapped in any of the requested indices")
        );
    }
}
