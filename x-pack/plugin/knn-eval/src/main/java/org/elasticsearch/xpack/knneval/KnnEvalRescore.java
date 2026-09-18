/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.search.vectors.KnnSearchBuilder;

import java.util.Map;

/** Validates a DiskBBQ mapping and detects capped rescore windows. */
record KnnEvalRescore(@Nullable Float mappingOversample, boolean autoCalibrate) {

    static final String TYPE_FIELD = "type";
    static final String INDEX_OPTIONS_FIELD = "index_options";
    private static final String RESCORE_VECTOR_FIELD = "rescore_vector";
    private static final String OVERSAMPLE_FIELD = "oversample";
    private static final String AUTO_CALIBRATE_FIELD = "auto_calibrate";

    static final int MAX_NUM_CANDIDATES = KnnSearchBuilder.NUM_CANDS_LIMIT;

    static KnnEvalRescore fromFieldMapping(String field, @Nullable Map<String, Object> fieldMapping) {
        Object indexOptionsValue = fieldMapping == null ? null : fieldMapping.get(INDEX_OPTIONS_FIELD);
        if (indexOptionsValue instanceof Map<?, ?> == false) {
            throw new IllegalArgumentException(
                "field [" + field + "] must use [index_options.type=bbq_disk], found [" + indexOptionsValue + "]"
            );
        }
        Map<?, ?> indexOptions = (Map<?, ?>) indexOptionsValue;
        if ("bbq_disk".equals(indexOptions.get(TYPE_FIELD)) == false) {
            throw new IllegalArgumentException(
                "field [" + field + "] must use [index_options.type=bbq_disk], found [" + indexOptions.get(TYPE_FIELD) + "]"
            );
        }
        Float mappingOversample = null;
        if (indexOptions.get(RESCORE_VECTOR_FIELD) instanceof Map<?, ?> rescoreVector
            && rescoreVector.get(OVERSAMPLE_FIELD) instanceof Number oversample) {
            mappingOversample = oversample.floatValue();
        }
        return new KnnEvalRescore(mappingOversample, Boolean.TRUE.equals(indexOptions.get(AUTO_CALIBRATE_FIELD)));
    }

    boolean isRescoreWindowCapped(int searchSize, @Nullable Float knobOversample) {
        Float oversample = effectiveOversample(knobOversample);
        return oversample != null && Math.ceil(searchSize * oversample) > DenseVectorFieldMapper.OVERSAMPLE_LIMIT;
    }

    @Nullable
    private Float effectiveOversample(@Nullable Float knobOversample) {
        return knobOversample == null ? mappingOversample : knobOversample;
    }
}
