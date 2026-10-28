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

    /**
     * The baseline's depth as a multiple of k: graphs search this many times k, and quantized baselines rescore that same
     * window, so the beam and the full-precision window are one number. DiskBBQ's visit is a percentage, so it shares only the rescore.
     */
    static final int BASELINE_DEPTH_FACTOR = 100;
    private static final float BASELINE_OVERSAMPLE = BASELINE_DEPTH_FACTOR;
    private static final float BBQ_DISK_BASELINE_VISIT_PERCENTAGE = 20.0f;
    /** The shallowest default beam, for small k. */
    static final int MIN_BASELINE_NUM_CANDIDATES = 1000;

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
     * Bounded, never exact: a beam of {@link #BASELINE_DEPTH_FACTOR} times k for graphs (1,000 to the search limit), a fifth of
     * the postings for DiskBBQ. Deeper candidates are rejected by {@link #validateDepth}, so these need an explicit baseline.
     * DiskBBQ's baseline leaves {@code num_candidates} unset because an explicit {@code visit_percentage} makes it inert.
     */
    KnnEvalSettings defaultBaseline(int k, boolean sampling) {
        if (isGraph() == false) {
            return new KnnEvalSettings(BBQ_DISK_BASELINE_VISIT_PERCENTAGE, null, BASELINE_OVERSAMPLE, false);
        }
        // a sampled query also retrieves its own document: one extra candidate
        int limit = sampling ? MAX_NUM_CANDIDATES - 1 : MAX_NUM_CANDIDATES;
        int numCandidates = Math.clamp((long) BASELINE_DEPTH_FACTOR * k, MIN_BASELINE_NUM_CANDIDATES, limit);
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
        } else if (isGraph() == false && hasFixedVisit(settings) && settings.getNumCandidates() != null) {
            problem = "[num_candidates] has no effect on bbq_disk with a positive [visit_percentage]; set only one";
        } else if (isQuantized() == false && settings.getRescoreOversample() != null) {
            problem = "[rescore_vector] does not apply to an unquantized [hnsw] field";
        }
        if (problem != null) {
            throw new IllegalArgumentException(problem + ", in the " + role + " " + settings);
        }
    }

    /**
     * Rejects a candidate that searches deeper than an approximate baseline: true neighbors only the candidate finds would
     * count as its misses. A DiskBBQ search without {@code visit_percentage} picks its depth per query, so it can't be compared.
     */
    void validateDepth(KnnEvalSettings baseline, KnnEvalSettings candidate, int k) {
        if (baseline.isExact()) {
            return;
        }
        String problem = null;
        if (isGraph()) {
            int baselineBeam = graphBeam(baseline, k);
            int candidateBeam = graphBeam(candidate, k);
            if (candidateBeam > baselineBeam) {
                problem = Strings.format("its search beam [%d] is wider than the baseline's [%d]", candidateBeam, baselineBeam);
            }
        } else if (hasFixedVisit(baseline) && hasFixedVisit(candidate) && candidate.getVisitPercentage() > baseline.getVisitPercentage()) {
            problem = "its [visit_percentage] is higher than the baseline's";
        }
        if (problem != null) {
            throw new IllegalArgumentException(
                Strings.format(
                    "knn_settings entry %s searches deeper than the baseline %s: %s; deepen the baseline or use [exact]",
                    candidate,
                    baseline,
                    problem
                )
            );
        }
    }

    /** Unset or 0 lets DiskBBQ pick the visit ratio per query. */
    private static boolean hasFixedVisit(KnnEvalSettings settings) {
        return settings.getVisitPercentage() != null && settings.getVisitPercentage() > 0;
    }

    /** Per segment, the graph search collects {@code max(num_candidates, k * oversample)} when it rescores. */
    private int graphBeam(KnnEvalSettings settings, int k) {
        int numCandidates = settings.getNumCandidates() != null
            ? settings.getNumCandidates()
            : Math.round(Math.min(KnnSearchBuilder.NUM_CANDS_LIMIT, KnnSearchBuilder.NUM_CANDS_MULTIPLICATIVE_FACTOR * k));
        Float oversample = effectiveOversample(settings.getRescoreOversample());
        if (isQuantized() && oversample != null && oversample > 0) {
            return Math.max(numCandidates, Math.min((int) Math.ceil(k * oversample), DenseVectorFieldMapper.OVERSAMPLE_LIMIT));
        }
        return numCandidates;
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
