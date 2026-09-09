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
import java.util.Optional;

/**
 * Works out what a kNN search will <em>actually</em> do, given a field's mapping and one set of knobs.
 * <p>
 * The numbers a caller asks for are not the numbers that run. A quantized field widens the candidate window to
 * {@code ceil(k * oversample)} so it has something to rescore, and both that window and {@code num_candidates} are capped. Echoing the
 * resolved values back saves a caller reverse-engineering why two knob sets that look different behaved identically -- most often
 * because both saturated the cap.
 *
 * @param mappingOversample the field's own {@code index_options.rescore_vector.oversample}, or {@code null} if it has none
 * @param quantized         whether the index type stores quantized vectors; rescoring is only ever applied when it does
 */
record KnnEvalRescore(@Nullable Float mappingOversample, boolean quantized) {

    /** Mirrors {@code DenseVectorFieldMapper.DenseVectorFieldType#createKnnFloatQuery}'s window widening. */
    private static final int OVERSAMPLE_LIMIT = DenseVectorFieldMapper.OVERSAMPLE_LIMIT;
    private static final int NUM_CANDS_LIMIT = KnnSearchBuilder.NUM_CANDS_LIMIT;
    private static final float NUM_CANDS_MULTIPLICATIVE_FACTOR = KnnSearchBuilder.NUM_CANDS_MULTIPLICATIVE_FACTOR;

    static KnnEvalRescore fromFieldMapping(Map<String, Object> fieldMapping) {
        Float mappingOversample = null;
        boolean quantized = false;
        if (fieldMapping.get(KnnEvalFidelity.INDEX_OPTIONS_FIELD) instanceof Map<?, ?> indexOptions) {
            if (indexOptions.get(KnnEvalFidelity.TYPE_FIELD) instanceof String type) {
                Optional<VectorIndexType> indexType = VectorIndexType.fromString(type);
                quantized = indexType.isPresent() && indexType.get().isQuantized();
            }
            if (indexOptions.get(KnnEvalFidelity.RESCORE_VECTOR_FIELD) instanceof Map<?, ?> rescoreVector
                && rescoreVector.get(KnnEvalFidelity.OVERSAMPLE_FIELD) instanceof Number oversample) {
                mappingOversample = oversample.floatValue();
            }
        }
        return new KnnEvalRescore(mappingOversample, quantized);
    }

    /**
     * The number of candidates that will be rescored on the real vectors, or 0 when nothing is rescored.
     *
     * @param searchSize the {@code k} the action actually sends, which is one more than the request's {@code k} for sampled queries
     */
    int rescoreWindow(int searchSize, @Nullable Float knobOversample) {
        Float oversample = knobOversample == null ? mappingOversample : knobOversample;
        if (quantized == false || oversample == null || oversample <= 0.0f) {
            return 0;
        }
        return Math.min((int) Math.ceil(searchSize * oversample), OVERSAMPLE_LIMIT);
    }

    /**
     * The candidate window the shard will collect: whatever was asked for, never below the rescore window, and capped.
     */
    int effectiveNumCandidates(int searchSize, @Nullable Integer knobNumCandidates, @Nullable Float knobOversample) {
        int numCandidates = knobNumCandidates == null
            ? Math.round(Math.min(NUM_CANDS_LIMIT, NUM_CANDS_MULTIPLICATIVE_FACTOR * searchSize))
            : Math.max(knobNumCandidates, searchSize);
        return Math.min(Math.max(numCandidates, rescoreWindow(searchSize, knobOversample)), NUM_CANDS_LIMIT);
    }
}
