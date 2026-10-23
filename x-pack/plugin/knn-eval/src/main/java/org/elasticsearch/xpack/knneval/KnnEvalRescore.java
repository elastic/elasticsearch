/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.RescoreVectorBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Validates a DiskBBQ or HNSW-family mapping, picks the matching baseline, and detects capped rescore windows. */
record KnnEvalRescore(IndexType indexType, @Nullable Float mappingOversample, boolean autoCalibrate) {

    /** Index types whose search settings differ, so each needs its own validation and default baseline. */
    enum IndexType {
        BBQ_DISK("bbq_disk", false, true),
        HNSW("hnsw", true, false),
        INT8_HNSW("int8_hnsw", true, true),
        INT4_HNSW("int4_hnsw", true, true),
        BBQ_HNSW("bbq_hnsw", true, true);

        private final String mappingName;
        private final boolean graph;
        /** Hits are scored on quantized vectors unless something rescores them. */
        private final boolean quantized;

        IndexType(String mappingName, boolean graph, boolean quantized) {
            this.mappingName = mappingName;
            this.graph = graph;
            this.quantized = quantized;
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
            return graph;
        }

        boolean isQuantized() {
            return quantized;
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
        boolean hasIndexOptions = fieldMapping != null && fieldMapping.get(INDEX_OPTIONS_FIELD) instanceof Map<?, ?>;
        Map<?, ?> indexOptions = hasIndexOptions ? (Map<?, ?>) fieldMapping.get(INDEX_OPTIONS_FIELD) : Map.of();
        IndexType indexType = IndexType.fromMappingName(indexOptions.get(TYPE_FIELD));
        if (indexType == null) {
            String found = hasIndexOptions == false ? Strings.format("no [%s] in mapping [%s]", INDEX_OPTIONS_FIELD, fieldMapping)
                : indexOptions.get(TYPE_FIELD) == null ? Strings.format("no [%s.%s]", INDEX_OPTIONS_FIELD, TYPE_FIELD)
                : Strings.format("[%s]", indexOptions.get(TYPE_FIELD));
            throw new IllegalArgumentException(
                Strings.format("field [%s] must use [%s.%s] of %s, found %s", field, INDEX_OPTIONS_FIELD, TYPE_FIELD, SUPPORTED, found)
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
     * Bounded, never exact: the deepest beam for graphs, a fifth of the postings for DiskBBQ. DiskBBQ's baseline leaves
     * {@code num_candidates} unset because an explicit {@code visit_percentage} makes it inert.
     */
    KnnEvalSettings defaultBaseline(boolean sampling) {
        if (indexType.isGraph() == false) {
            return new KnnEvalSettings(BBQ_DISK_BASELINE_VISIT_PERCENTAGE, null, BASELINE_OVERSAMPLE, false);
        }
        // a sampled query also retrieves its own document: one extra candidate
        int numCandidates = sampling ? MAX_NUM_CANDIDATES - 1 : MAX_NUM_CANDIDATES;
        return new KnnEvalSettings(null, numCandidates, indexType.isQuantized() ? BASELINE_OVERSAMPLE : null, false);
    }

    /** Rejects a setting of the other index family, which would be ignored or change what the setting means. */
    void validateSettings(KnnEvalSettings settings, String role) {
        String problem = null;
        if (indexType.isGraph() && settings.getVisitPercentage() != null) {
            problem = Strings.format(
                "[visit_percentage] applies only to bbq_disk, but the field is [%s]; use [num_candidates]",
                indexType.mappingName
            );
        } else if (indexType.isQuantized() == false && settings.getRescoreOversample() != null) {
            problem = "[rescore_vector] does not apply to an unquantized [hnsw] field";
        }
        if (problem != null) {
            throw new IllegalArgumentException(problem + ", in the " + role + " " + settings);
        }
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
