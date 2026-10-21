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
import org.elasticsearch.search.vectors.RescoreVectorBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Validates a DiskBBQ or HNSW-family mapping, picks the matching baseline, and detects capped rescore windows. */
record KnnEvalRescore(IndexType indexType, @Nullable Float mappingOversample, boolean autoCalibrate) {

    /** Index types whose search knobs differ, so each needs its own validation and default baseline. */
    enum IndexType {
        BBQ_DISK("bbq_disk", false),
        HNSW("hnsw", false),
        INT8_HNSW("int8_hnsw", true),
        INT4_HNSW("int4_hnsw", true),
        BBQ_HNSW("bbq_hnsw", true);

        private final String mappingName;
        private final boolean quantizedGraph;

        IndexType(String mappingName, boolean quantizedGraph) {
            this.mappingName = mappingName;
            this.quantizedGraph = quantizedGraph;
        }

        @Nullable
        static IndexType fromMappingName(Object name) {
            for (IndexType type : values()) {
                if (type.mappingName.equals(name)) {
                    return type;
                }
            }
            return null;
        }

        boolean isGraph() {
            return this != BBQ_DISK;
        }

        /** Whether hits are scored on quantized vectors unless something rescores them. */
        boolean isQuantized() {
            return this == BBQ_DISK || quantizedGraph;
        }
    }

    static final String TYPE_FIELD = "type";
    static final String INDEX_OPTIONS_FIELD = "index_options";
    private static final String AUTO_CALIBRATE_FIELD = "auto_calibrate";

    private static final List<String> SUPPORTED = Arrays.stream(IndexType.values()).map(t -> t.mappingName).toList();

    /** Quantized baselines rescore deeply so their reference scores are full precision. */
    private static final float BASELINE_OVERSAMPLE = 100.0f;
    private static final float BBQ_DISK_BASELINE_VISIT_PERCENTAGE = 20.0f;

    static final int MAX_NUM_CANDIDATES = KnnSearchBuilder.NUM_CANDS_LIMIT;

    static KnnEvalRescore fromFieldMapping(String field, @Nullable Map<String, Object> fieldMapping) {
        Object indexOptionsValue = fieldMapping == null ? null : fieldMapping.get(INDEX_OPTIONS_FIELD);
        if (indexOptionsValue instanceof Map<?, ?> == false) {
            throw new IllegalArgumentException(
                "field ["
                    + field
                    + "] must use [index_options.type] of one of "
                    + SUPPORTED
                    + ", found no [index_options] in mapping ["
                    + fieldMapping
                    + "]"
            );
        }
        Map<?, ?> indexOptions = (Map<?, ?>) indexOptionsValue;
        IndexType indexType = IndexType.fromMappingName(indexOptions.get(TYPE_FIELD));
        if (indexType == null) {
            throw new IllegalArgumentException(
                "field ["
                    + field
                    + "] must use [index_options.type] of one of "
                    + SUPPORTED
                    + ", found ["
                    + indexOptions.get(TYPE_FIELD)
                    + "]"
            );
        }
        Float mappingOversample = null;
        if (indexOptions.get(KnnSearchBuilder.RESCORE_VECTOR_FIELD.getPreferredName()) instanceof Map<?, ?> rescoreVector
            && rescoreVector.get(RescoreVectorBuilder.OVERSAMPLE_FIELD.getPreferredName()) instanceof Number oversample) {
            mappingOversample = oversample.floatValue();
        }
        return new KnnEvalRescore(
            indexType,
            mappingOversample,
            indexType == IndexType.BBQ_DISK && Boolean.TRUE.equals(indexOptions.get(AUTO_CALIBRATE_FIELD))
        );
    }

    /**
     * The bounded reference used when the request names none: never exact, which brute-forces every vector. A graph search
     * takes the deepest beam allowed, DiskBBQ a fifth of its postings.
     */
    KnnEvalSettings defaultBaseline(boolean sampling) {
        if (indexType.isGraph() == false) {
            return new KnnEvalSettings(BBQ_DISK_BASELINE_VISIT_PERCENTAGE, null, BASELINE_OVERSAMPLE, false);
        }
        // a sampled query also retrieves its own document: one extra candidate
        int numCandidates = sampling ? MAX_NUM_CANDIDATES - 1 : MAX_NUM_CANDIDATES;
        return new KnnEvalSettings(null, numCandidates, indexType.isQuantized() ? BASELINE_OVERSAMPLE : null, false);
    }

    /** Rejects a knob of the other index family, which would be ignored or change what the setting means. */
    void validateSettings(KnnEvalSettings settings, String role) {
        String visitPercentage = KnnEvalSettings.VISIT_PERCENTAGE_FIELD.getPreferredName();
        String numCandidates = KnnEvalSettings.NUM_CANDIDATES_FIELD.getPreferredName();
        if (indexType.isGraph() && settings.getVisitPercentage() != null) {
            throw mismatch(visitPercentage, "bbq_disk", numCandidates, role, settings);
        }
        if (indexType.isGraph() == false && settings.getNumCandidates() != null) {
            throw mismatch(numCandidates, "the hnsw family", visitPercentage, role, settings);
        }
        if (indexType.isQuantized() == false && settings.getRescoreOversample() != null) {
            throw new IllegalArgumentException(
                "["
                    + KnnEvalSettings.RESCORE_VECTOR_FIELD.getPreferredName()
                    + "] does not apply to an unquantized [hnsw] field, in the "
                    + role
                    + " "
                    + settings
            );
        }
    }

    private IllegalArgumentException mismatch(String knob, String appliesTo, String instead, String role, KnnEvalSettings settings) {
        return new IllegalArgumentException(
            "["
                + knob
                + "] applies only to "
                + appliesTo
                + ", but the field is ["
                + indexType.mappingName
                + "]; use ["
                + instead
                + "] in the "
                + role
                + " "
                + settings
        );
    }

    /** True if nothing rescores these hits, so their scores are estimates. Exact searches are always full precision. */
    boolean returnsQuantizedScores(KnnEvalSettings knnSettings) {
        return indexType.isQuantized()
            && knnSettings.isExact() == false
            && autoCalibrate == false
            && effectiveOversample(knnSettings.getRescoreOversample()) == null;
    }

    boolean isRescoreWindowCapped(int searchSize, @Nullable Float requestedOversample) {
        Float oversample = effectiveOversample(requestedOversample);
        return oversample != null && Math.ceil(searchSize * oversample) > DenseVectorFieldMapper.OVERSAMPLE_LIMIT;
    }

    @Nullable
    private Float effectiveOversample(@Nullable Float requestedOversample) {
        return requestedOversample == null ? mappingOversample : requestedOversample;
    }
}
