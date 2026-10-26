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
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorIndexType;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.RescoreVectorBuilder;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The evaluated field's index type and mapped rescoring, which decide which settings apply, the default baseline, and whether
 * scores are quantized estimates.
 */
record KnnEvalFieldType(VectorIndexType indexType, @Nullable Float mappingOversample, boolean autoCalibrate) {

    /** Types with a search effort to vary; flat types scan every vector. */
    static final Set<VectorIndexType> SUPPORTED = EnumSet.of(
        VectorIndexType.BBQ_DISK,
        VectorIndexType.HNSW,
        VectorIndexType.INT8_HNSW,
        VectorIndexType.INT4_HNSW,
        VectorIndexType.BBQ_HNSW
    );

    static final String TYPE_FIELD = "type";
    static final String INDEX_OPTIONS_FIELD = "index_options";
    private static final String AUTO_CALIBRATE_FIELD = "auto_calibrate";

    private static final List<String> SUPPORTED_NAMES = SUPPORTED.stream().map(VectorIndexType::getName).toList();

    /** Quantized baselines rescore deeply so their reference scores are full precision. */
    private static final float BASELINE_OVERSAMPLE = 100.0f;
    private static final float BBQ_DISK_BASELINE_VISIT_PERCENTAGE = 20.0f;

    static final int MAX_NUM_CANDIDATES = KnnSearchBuilder.NUM_CANDS_LIMIT;

    static KnnEvalFieldType fromFieldMapping(String field, @Nullable Map<String, Object> fieldMapping) {
        boolean hasIndexOptions = fieldMapping != null && fieldMapping.get(INDEX_OPTIONS_FIELD) instanceof Map<?, ?>;
        Map<?, ?> indexOptions = hasIndexOptions ? (Map<?, ?>) fieldMapping.get(INDEX_OPTIONS_FIELD) : Map.of();
        Object typeName = indexOptions.get(TYPE_FIELD);
        VectorIndexType indexType = typeName instanceof String name
            ? VectorIndexType.fromString(name).filter(SUPPORTED::contains).orElse(null)
            : null;
        if (indexType == null) {
            String found = hasIndexOptions == false ? Strings.format("no [%s] in mapping [%s]", INDEX_OPTIONS_FIELD, fieldMapping)
                : typeName == null ? Strings.format("no [%s.%s]", INDEX_OPTIONS_FIELD, TYPE_FIELD)
                : Strings.format("[%s]", typeName);
            throw new IllegalArgumentException(
                Strings.format(
                    "field [%s] must use [%s.%s] of %s, found %s",
                    field,
                    INDEX_OPTIONS_FIELD,
                    TYPE_FIELD,
                    SUPPORTED_NAMES,
                    found
                )
            );
        }
        Float mappingOversample = null;
        if (indexOptions.get(KnnSearchBuilder.RESCORE_VECTOR_FIELD.getPreferredName()) instanceof Map<?, ?> rescoreVector
            && rescoreVector.get(RescoreVectorBuilder.OVERSAMPLE_FIELD.getPreferredName()) instanceof Number oversample) {
            mappingOversample = oversample.floatValue();
        }
        return new KnnEvalFieldType(
            indexType,
            mappingOversample,
            indexType == VectorIndexType.BBQ_DISK && Boolean.TRUE.equals(indexOptions.get(AUTO_CALIBRATE_FIELD))
        );
    }

    /** HNSW-family: searched with {@code num_candidates}, not {@code visit_percentage}. */
    boolean isGraph() {
        return indexType != VectorIndexType.BBQ_DISK;
    }

    /** Hits are scored on quantized vectors unless something rescores them. */
    boolean isQuantized() {
        return indexType.isQuantized();
    }

    /**
     * Bounded, never exact: the deepest beam for graphs, a fifth of the postings for DiskBBQ. DiskBBQ's baseline leaves
     * {@code num_candidates} unset because an explicit {@code visit_percentage} makes it inert.
     */
    KnnEvalSettings defaultBaseline(boolean sampling) {
        if (isGraph() == false) {
            return new KnnEvalSettings(BBQ_DISK_BASELINE_VISIT_PERCENTAGE, null, BASELINE_OVERSAMPLE, false);
        }
        // a sampled query also retrieves its own document: one extra candidate
        int numCandidates = sampling ? MAX_NUM_CANDIDATES - 1 : MAX_NUM_CANDIDATES;
        return new KnnEvalSettings(null, numCandidates, isQuantized() ? BASELINE_OVERSAMPLE : null, false);
    }

    /** Rejects a setting the search would silently ignore for this index type. */
    void validateSettings(KnnEvalSettings settings, String role) {
        String problem = null;
        if (isGraph() && settings.getVisitPercentage() != null) {
            problem = Strings.format(
                "[visit_percentage] applies only to bbq_disk, but the field is [%s]; use [num_candidates]",
                indexType.getName()
            );
        } else if (isQuantized() == false && settings.getRescoreOversample() != null) {
            problem = "[rescore_vector] does not apply to an unquantized [hnsw] field";
        }
        if (problem != null) {
            throw new IllegalArgumentException(problem + ", in the " + role + " " + settings);
        }
    }

    /** True if nothing rescores these hits, so their scores are estimates. Exact searches are always full precision. */
    boolean returnsQuantizedScores(KnnEvalSettings knnSettings) {
        return isQuantized()
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
