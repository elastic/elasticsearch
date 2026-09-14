/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.knneval;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorSimilarity;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Inverts {@code _score} back into the similarity the fidelity metric is defined on: {@code epsilon_i = max(0, s(i)/t(i) - 1)} for the
 * baseline's similarity {@code s} and the candidate's {@code t}. Scores cannot be used directly because {@link VectorSimilarity}
 * applies a non-linear transform, so a score ratio is not a similarity ratio. The inversions mirror {@code VectorSimilarity#score},
 * which is not visible outside its own package. For {@code l2_norm} the quantity is a distance, so the ratio flips.
 */
public record KnnEvalFidelity(VectorSimilarity similarity, @Nullable String skippedReason) {

    static final String TYPE_FIELD = "type";
    static final String SIMILARITY_FIELD = "similarity";
    static final String ELEMENT_TYPE_FIELD = "element_type";
    static final String INDEX_OPTIONS_FIELD = "index_options";
    static final String RESCORE_VECTOR_FIELD = "rescore_vector";
    static final String OVERSAMPLE_FIELD = "oversample";

    static final String DIMS_FIELD = "dims";
    static final String DENSE_VECTOR_TYPE = "dense_vector";

    /** Without rescoring a quantized index scores the quantized vector, so a score ratio measures quantization error. */
    static final String RESCORING_DISABLED = "rescoring disabled; scores are quantized estimates";

    /** Only the float transforms invert to a single similarity; byte and bit fold in the dimension. */
    private static final List<ElementType> INVERTIBLE_ELEMENT_TYPES = List.of(ElementType.FLOAT, ElementType.BFLOAT16);

    /**
     * Reads what the metric needs out of one field's mapping.
     *
     * @param baselineOversample lifts the rescoring guard below, keyed on the baseline alone because it is the reference every epsilon
     *                           is measured against; a candidate without rescoring just scores badly
     * @throws IllegalArgumentException if the field is not a {@code dense_vector}
     */
    @SuppressWarnings("unchecked")
    static KnnEvalFidelity fromFieldMapping(
        String field,
        @Nullable Map<String, Object> fieldMapping,
        @Nullable KnnEvalRescore rescore,
        @Nullable Float baselineOversample
    ) {
        if (fieldMapping == null) {
            throw new IllegalArgumentException("field [" + field + "] is not mapped in any of the requested indices");
        }
        Object type = fieldMapping.get(TYPE_FIELD);
        if (DENSE_VECTOR_TYPE.equals(type) == false) {
            throw new IllegalArgumentException(
                "field [" + field + "] is of type [" + type + "], but [" + DENSE_VECTOR_TYPE + "] is required"
            );
        }
        // an indexed dense_vector always renders its similarity; default rather than fail if a future mapping omits it
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

        // only a quantized index with rescoring off reports estimates; an unquantized one already scores on real vectors
        boolean rescored = baselineOversample != null
            || rescore == null
            || rescore.quantized() == false
            || (rescore.mappingOversample() != null && rescore.mappingOversample() > 0.0f);
        return new KnnEvalFidelity(similarity, rescored ? null : RESCORING_DISABLED);
    }

    /** The mapping could not be read, so nothing similarity-based can be computed. */
    static KnnEvalFidelity unavailable(String reason) {
        return new KnnEvalFidelity(VectorSimilarity.COSINE, reason);
    }

    boolean isSkipped() {
        return skippedReason != null;
    }

    /** A distance (smaller is better) flips the direction of the ratio. */
    boolean isDistanceBased() {
        return similarity == VectorSimilarity.L2_NORM;
    }

    /** @return the similarity, or the Euclidean distance for {@code l2_norm} */
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
     * Whether a hit is as good <em>in value</em> as the worst the baseline accepted, which separates a different-but-equally-good
     * document from a worse one. On a corpus with duplicates or dense ties the two recalls diverge sharply.
     *
     * @param tolerance multiplicative slack; for {@code max_inner_product}, where similarities can be negative, it tightens the
     *                  threshold rather than loosening it
     */
    boolean isValueMatch(float baselineWorstScore, float candidateScore, double tolerance) {
        double threshold = invert(baselineWorstScore);
        double candidate = invert(candidateScore);
        if (isDistanceBased()) {
            return candidate <= threshold * (1.0 + tolerance);
        }
        return candidate >= threshold * (1.0 - tolerance);
    }

    /** @return {@code null} when no finite ratio exists: a non-positive candidate similarity, or a zero baseline distance */
    @Nullable
    Double epsilonAtRank(float baselineScore, float candidateScore) {
        double baseline = invert(baselineScore);
        double candidate = invert(candidateScore);
        if (isDistanceBased()) {
            if (baseline == 0.0) {
                // the baseline found an exact match, so only an exact match is as good
                return candidate == 0.0 ? 0.0 : null;
            }
            return Math.max(0.0, candidate / baseline - 1.0);
        }
        if (candidate <= 0.0) {
            return null;
        }
        return Math.max(0.0, baseline / candidate - 1.0);
    }

    /**
     * How much similarity was given up on the documents that were returned. Recall counts documents; this measures the gap, separating
     * a near-miss from something unrelated. Zero loss at every rank is equivalent to recall 1.0.
     * <p>
     * The reference is the {@code baseline}, so zero means "as good as the baseline" and not "optimal", and the metric is skipped --
     * {@link #skipped} carries the reason -- when the field's scores are quantized estimates.
     *
     * @param maxEpsilon        per-query worst loss; {@code p95} is the usual dashboard scalar
     * @param infiniteCount     queries excluded from {@code maxEpsilon} rather than clamped
     * @param meanEpsilonByRank {@code null} at a rank no query reached
     */
    public record Summary(@Nullable String skipped, @Nullable KnnEvalStats maxEpsilon, long infiniteCount, List<Double> meanEpsilonByRank)
        implements
            Writeable,
            ToXContentObject {

        static final ParseField SKIPPED_FIELD = new ParseField("skipped");
        static final ParseField MAX_EPSILON_FIELD = new ParseField("max_epsilon");
        static final ParseField INFINITE_COUNT_FIELD = new ParseField("infinite_count");
        static final ParseField MEAN_EPSILON_BY_RANK_FIELD = new ParseField("mean_epsilon_by_rank");

        public static Summary skipped(String reason) {
            return new Summary(Objects.requireNonNull(reason), null, 0, List.of());
        }

        public static Summary of(KnnEvalStats maxEpsilon, long infiniteCount, List<Double> meanEpsilonByRank) {
            return new Summary(null, Objects.requireNonNull(maxEpsilon), infiniteCount, meanEpsilonByRank);
        }

        public Summary(@Nullable String skipped, @Nullable KnnEvalStats maxEpsilon, long infiniteCount, List<Double> meanEpsilonByRank) {
            this.skipped = skipped;
            this.maxEpsilon = maxEpsilon;
            this.infiniteCount = infiniteCount;
            // nulls are meaningful here, so this cannot be List#copyOf
            this.meanEpsilonByRank = Collections.unmodifiableList(new ArrayList<>(meanEpsilonByRank));
        }

        Summary(StreamInput in) throws IOException {
            this(
                in.readOptionalString(),
                in.readOptionalWriteable(KnnEvalStats::new),
                in.readVLong(),
                in.readCollectionAsList(StreamInput::readOptionalDouble)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(skipped);
            out.writeOptionalWriteable(maxEpsilon);
            out.writeVLong(infiniteCount);
            out.writeCollection(meanEpsilonByRank, StreamOutput::writeOptionalDouble);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (skipped != null) {
                builder.field(SKIPPED_FIELD.getPreferredName(), skipped);
                builder.endObject();
                return builder;
            }
            builder.field(MAX_EPSILON_FIELD.getPreferredName());
            maxEpsilon.toXContent(builder, params);
            builder.field(INFINITE_COUNT_FIELD.getPreferredName(), infiniteCount);
            builder.startArray(MEAN_EPSILON_BY_RANK_FIELD.getPreferredName());
            for (Double epsilon : meanEpsilonByRank) {
                builder.value(epsilon);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }
    }
}
