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
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorIndexType;
import org.elasticsearch.search.vectors.KnnSearchBuilder;

import java.util.Map;

/**
 * What a kNN search will <em>actually</em> do with a knob set, given the field's mapping. The numbers asked for are not the numbers that
 * run: a quantized field widens the candidate window to {@code ceil(k * oversample)}, and both that and {@code num_candidates} are
 * capped, so two knob sets that look different can behave identically. This is also where a knob the index type would ignore is
 * rejected.
 *
 * @param indexType {@code null} when the mapping states none. The default is version, dimension and element-type dependent and a
 *                  field-mappings response carries none of that, so an absent type means unknown and the checks are skipped.
 */
record KnnEvalRescore(@Nullable VectorIndexType indexType, @Nullable Float mappingOversample) {

    /** Mirrors {@code DenseVectorFieldMapper.DenseVectorFieldType#createKnnFloatQuery}'s window widening. */
    private static final int OVERSAMPLE_LIMIT = DenseVectorFieldMapper.OVERSAMPLE_LIMIT;
    private static final int NUM_CANDS_LIMIT = KnnSearchBuilder.NUM_CANDS_LIMIT;
    private static final float NUM_CANDS_MULTIPLICATIVE_FACTOR = KnnSearchBuilder.NUM_CANDS_MULTIPLICATIVE_FACTOR;

    static KnnEvalRescore fromFieldMapping(Map<String, Object> fieldMapping) {
        VectorIndexType indexType = null;
        Float mappingOversample = null;
        if (fieldMapping.get(KnnEvalFidelity.INDEX_OPTIONS_FIELD) instanceof Map<?, ?> indexOptions) {
            if (indexOptions.get(KnnEvalFidelity.TYPE_FIELD) instanceof String type) {
                indexType = VectorIndexType.fromString(type).orElse(null);
            }
            if (indexOptions.get(KnnEvalFidelity.RESCORE_VECTOR_FIELD) instanceof Map<?, ?> rescoreVector
                && rescoreVector.get(KnnEvalFidelity.OVERSAMPLE_FIELD) instanceof Number oversample) {
                mappingOversample = oversample.floatValue();
            }
        }
        return new KnnEvalRescore(indexType, mappingOversample);
    }

    /** Whether a rescore pass is ever applied. */
    boolean quantized() {
        return indexType != null && indexType.isQuantized();
    }

    /**
     * Rejects a knob the field's index type would ignore: a {@code visit_percentage} sweep against an HNSW field runs the same search
     * every time and reports a flat recall of 1.0, which reads as "the cheap setting is free".
     *
     * @throws IllegalArgumentException if a knob does not apply to the field
     */
    void validateSupportedKnobs(KnnEvalKnobs knobs) {
        if (indexType == null || knobs.isExact()) {
            // nothing to validate against, or nothing approximate to configure
            return;
        }
        boolean visitPercentageSupported = switch (indexType) {
            // only the IVF family has posting lists to visit a fraction of
            case BBQ_DISK -> true;
            case HNSW, INT8_HNSW, INT4_HNSW, BBQ_HNSW, FLAT, INT8_FLAT, INT4_FLAT, BBQ_FLAT -> false;
        };
        if (visitPercentageSupported == false && knobs.getVisitPercentage() != null) {
            // 0 means "let the index choose" rather than "unset", so it counts as present
            throw new IllegalArgumentException(
                "["
                    + KnnEvalKnobs.VISIT_PERCENTAGE_FIELD.getPreferredName()
                    + "] is not supported for index_options type ["
                    + indexType
                    + "]; use ["
                    + KnnEvalKnobs.NUM_CANDIDATES_FIELD.getPreferredName()
                    + "]"
            );
        }
        if (quantized() == false && knobs.getOversample() != null) {
            // a query-time rescore_vector is silently ignored on unquantized fields
            throw new IllegalArgumentException(
                "["
                    + KnnEvalKnobs.OVERSAMPLE_FIELD.getPreferredName()
                    + "] has no effect on unquantized index_options type ["
                    + indexType
                    + "]"
            );
        }
        // num_candidates applies everywhere: the graph families walk it, the IVF family derives its visit ratio from it
    }

    /**
     * The candidates rescored on the real vectors, or 0 when nothing is.
     *
     * @param searchSize the {@code k} actually sent, one more than the request's {@code k} for sampled queries
     */
    int rescoreWindow(int searchSize, @Nullable Float knobOversample) {
        Float oversample = knobOversample == null ? mappingOversample : knobOversample;
        if (quantized() == false || oversample == null || oversample <= 0.0f) {
            return 0;
        }
        return Math.min((int) Math.ceil(searchSize * oversample), OVERSAMPLE_LIMIT);
    }

    /** Whatever was asked for, floored by the rescore window and capped. */
    int effectiveNumCandidates(int searchSize, @Nullable Integer knobNumCandidates, @Nullable Float knobOversample) {
        int numCandidates = knobNumCandidates == null
            ? Math.round(Math.min(NUM_CANDS_LIMIT, NUM_CANDS_MULTIPLICATIVE_FACTOR * searchSize))
            : Math.max(knobNumCandidates, searchSize);
        return Math.min(Math.max(numCandidates, rescoreWindow(searchSize, knobOversample)), NUM_CANDS_LIMIT);
    }
}
