/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorSimilarity;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Turns {@code _score} values back into the similarities (or distances) that the fidelity metric is defined on, and decides whether the
 * metric is meaningful at all for a given field.
 * <p>
 * Recall answers "did the candidate find the same documents?" but not "how much worse are the ones it found instead?". A candidate that
 * misses the true neighbour by a hair is very different from one that returns something unrelated, and both score the same recall.
 * Fidelity measures that gap: per rank {@code i}, {@code epsilon_i = max(0, s(i)/t(i) - 1)} where {@code s} is the baseline's similarity
 * at that rank and {@code t} the candidate's. Zero at every rank is equivalent to recall 1.0.
 * <p>
 * It has to be computed on similarities rather than on {@code _score}, because {@code _score} is a monotonic but non-linear transform of
 * the similarity ({@link VectorSimilarity} applies it), so a ratio of scores is not a ratio of similarities. The inversions below mirror
 * {@code VectorSimilarity#score}, which is not visible outside its own package.
 * <p>
 * For {@code l2_norm} the underlying quantity is a distance, where smaller is better, so the ratio is inverted:
 * {@code epsilon_i = max(0, t_d(i)/s_d(i) - 1)}.
 */
record KnnEvalFidelity(VectorSimilarity similarity, @Nullable String skippedReason) {

    static final String TYPE_FIELD = "type";
    static final String SIMILARITY_FIELD = "similarity";
    static final String ELEMENT_TYPE_FIELD = "element_type";
    static final String INDEX_OPTIONS_FIELD = "index_options";
    static final String RESCORE_VECTOR_FIELD = "rescore_vector";
    static final String OVERSAMPLE_FIELD = "oversample";

    static final String DENSE_VECTOR_TYPE = "dense_vector";

    /**
     * Without rescoring, a quantized index reports the score of the <em>quantized</em> vector rather than of the real one, so a score
     * ratio measures quantization error rather than search quality. Reporting a number there would be worse than reporting none.
     */
    static final String RESCORING_DISABLED = "rescoring disabled; scores are quantized estimates";

    /** Only the float element types have a score transform that inverts to a single similarity; byte and bit fold in the dimension. */
    private static final List<ElementType> INVERTIBLE_ELEMENT_TYPES = List.of(ElementType.FLOAT, ElementType.BFLOAT16);

    /**
     * Reads what the metric needs out of one field's mapping.
     *
     * @param fieldMapping the field's mapping body, i.e. the value under the field name in a field-mappings response
     * @param baselineOversample the baseline run's own oversample, if it set one. A run that asks for rescoring gets real scores even
     *                           when the mapping has rescoring off, so it lifts the guard below. This is deliberately keyed on the
     *                           baseline alone: it is the reference every epsilon is measured against, and a candidate without
     *                           rescoring simply scores badly rather than invalidating the metric.
     * @throws IllegalArgumentException if the field is not a {@code dense_vector}; evaluating kNN recall on anything else is a mistake
     *                                 worth reporting rather than silently degrading
     */
    @SuppressWarnings("unchecked")
    static KnnEvalFidelity fromFieldMapping(String field, @Nullable Map<String, Object> fieldMapping, @Nullable Float baselineOversample) {
        if (fieldMapping == null) {
            throw new IllegalArgumentException("field [" + field + "] is not mapped in any of the requested indices");
        }
        Object type = fieldMapping.get(TYPE_FIELD);
        if (DENSE_VECTOR_TYPE.equals(type) == false) {
            throw new IllegalArgumentException(
                "field [" + field + "] is of type [" + type + "], but [" + DENSE_VECTOR_TYPE + "] is required"
            );
        }
        // An indexed dense_vector always renders its similarity, but default to the mapper's own default rather than failing if a future
        // mapping omits it.
        Object similarityName = fieldMapping.get(SIMILARITY_FIELD);
        VectorSimilarity similarity = similarityName == null
            ? VectorSimilarity.COSINE
            : VectorSimilarity.valueOf(similarityName.toString().toUpperCase(Locale.ROOT));

        Object elementTypeName = fieldMapping.get(ELEMENT_TYPE_FIELD);
        ElementType elementType = elementTypeName == null
            ? ElementType.FLOAT
            : ElementType.valueOf(elementTypeName.toString().toUpperCase(Locale.ROOT));
        if (INVERTIBLE_ELEMENT_TYPES.contains(elementType) == false) {
            return new KnnEvalFidelity(similarity, "score inversion is only defined for float element types, not [" + elementType + "]");
        }

        if (baselineOversample == null
            && fieldMapping.get(INDEX_OPTIONS_FIELD) instanceof Map<?, ?> indexOptions
            && indexOptions.get(RESCORE_VECTOR_FIELD) instanceof Map<?, ?> rescoreVector
            && rescoreVector.get(OVERSAMPLE_FIELD) instanceof Number oversample
            && oversample.floatValue() == 0.0f) {
            return new KnnEvalFidelity(similarity, RESCORING_DISABLED);
        }
        return new KnnEvalFidelity(similarity, null);
    }

    /** The mapping could not be read, so nothing similarity-based can be computed. */
    static KnnEvalFidelity unavailable(String reason) {
        return new KnnEvalFidelity(VectorSimilarity.COSINE, reason);
    }

    boolean isSkipped() {
        return skippedReason != null;
    }

    /** True when the inverted quantity is a distance (smaller is better), which flips the direction of the ratio. */
    boolean isDistanceBased() {
        return similarity == VectorSimilarity.L2_NORM;
    }

    /**
     * Inverts {@code VectorSimilarity#score} for the float element types.
     *
     * @return the similarity for the inner-product families, or the Euclidean distance for {@code l2_norm}
     */
    double invert(float score) {
        return switch (similarity) {
            // score = 1 / (1 + d^2)
            case L2_NORM -> Math.sqrt(Math.max(0.0, 1.0 / score - 1.0));
            // score = (1 + sim) / 2
            case COSINE, DOT_PRODUCT -> 2.0 * score - 1.0;
            // score = sim < 0 ? 1 / (1 - sim) : sim + 1, so the two branches meet at score 1
            case MAX_INNER_PRODUCT -> score < 1.0f ? 1.0 - 1.0 / score : score - 1.0;
        };
    }

    /**
     * Whether a candidate hit is as good <em>in value</em> as the worst hit the baseline accepted.
     * <p>
     * This is what separates a candidate that found different-but-equally-good documents from one that found worse ones. On a corpus
     * with duplicates or dense ties the two recalls diverge sharply: id recall punishes picking the other copy of an identical vector,
     * value recall does not.
     *
     * @param tolerance multiplicative slack, so 0.05 accepts a similarity 5% below the baseline's k-th. Note that for
     *                  {@code max_inner_product}, where similarities can be negative, the slack tightens rather than loosens the
     *                  threshold -- the formula is multiplicative, not additive.
     */
    boolean isValueMatch(float baselineWorstScore, float candidateScore, double tolerance) {
        double threshold = invert(baselineWorstScore);
        double candidate = invert(candidateScore);
        if (isDistanceBased()) {
            return candidate <= threshold * (1.0 + tolerance);
        }
        return candidate >= threshold * (1.0 - tolerance);
    }

    /**
     * The fidelity loss at one rank.
     *
     * @return {@code max(0, s/t - 1)} (or the distance equivalent), or {@code null} when the loss is unbounded -- the candidate returned
     *         a non-positive similarity, or the baseline's distance was zero while the candidate's was not, so no finite ratio exists
     */
    @Nullable
    Double epsilonAtRank(float baselineScore, float candidateScore) {
        double baseline = invert(baselineScore);
        double candidate = invert(candidateScore);
        if (isDistanceBased()) {
            if (baseline == 0.0) {
                // the baseline found an exact match; only an exact match is as good
                return candidate == 0.0 ? 0.0 : null;
            }
            return Math.max(0.0, candidate / baseline - 1.0);
        }
        if (candidate <= 0.0) {
            return null;
        }
        return Math.max(0.0, baseline / candidate - 1.0);
    }
}
