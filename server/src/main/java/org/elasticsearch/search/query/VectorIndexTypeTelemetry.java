/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.query;

import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorIndexType;

/**
 * Bucketed {@code dense_vector} index type used for shard-level KNN telemetry. The granular
 * {@link VectorIndexType} values (e.g. {@code int8_hnsw}, {@code bbq_disk}) are collapsed to a
 * small set of buckets so the value fits in four bits of the {@link QuerySearchResult} wire
 * format. The wire encoding has 16 ordinals available; new buckets can be appended without a
 * transport version bump.
 *
 * <p>{@link #UNKNOWN} is reserved for ordinals that arrive from a newer node and are not yet
 * defined locally. Preserving the signal as {@code UNKNOWN} (rather than collapsing to
 * {@link #NONE}) lets dashboards distinguish "no KNN on this shard" from "this node doesn't
 * recognize the bucket the producer used", which matters during mixed-version rollouts.
 *
 * <p><b>Ordering is part of the wire format.</b> Ordinals are transmitted between nodes — only
 * append new values; never reorder or remove. {@code VectorIndexTypeTelemetryTests} pins the
 * current ordinals to catch accidental reorderings.
 */
public enum VectorIndexTypeTelemetry {
    NONE(null),
    UNKNOWN("unknown"),
    HNSW("hnsw"),
    FLAT("flat"),
    BBQ("bbq"),
    MIXED("mixed");

    private static final VectorIndexTypeTelemetry[] VALUES = values();

    private final String label;

    VectorIndexTypeTelemetry(String label) {
        this.label = label;
    }

    /**
     * APM attribute label for this bucket, or {@code null} for {@link #NONE}.
     */
    public String label() {
        return label;
    }

    /**
     * Decode a wire ordinal. Out-of-range values (a bucket defined on a newer node we don't
     * recognize) decode to {@link #UNKNOWN} so the signal is preserved rather than dropped.
     */
    public static VectorIndexTypeTelemetry fromOrdinal(int ordinal) {
        if (ordinal < 0 || ordinal >= VALUES.length) {
            return UNKNOWN;
        }
        return VALUES[ordinal];
    }

    /**
     * Maps a {@link VectorIndexType} to its telemetry bucket. Adding a new {@link VectorIndexType}
     * is a compile error here so the bucket assignment is an explicit decision.
     */
    public static VectorIndexTypeTelemetry of(VectorIndexType type) {
        return switch (type) {
            case HNSW, INT8_HNSW, INT4_HNSW -> HNSW;
            case FLAT, INT8_FLAT, INT4_FLAT -> FLAT;
            case BBQ_HNSW, BBQ_FLAT, BBQ_DISK -> BBQ;
        };
    }

    /**
     * Folds another bucket into this one. {@link #NONE} is the identity; two distinct non-{@link #NONE}
     * buckets collapse to {@link #MIXED}. Used both at recording time on a shard and again when the
     * coordinator reduces shard results.
     */
    public VectorIndexTypeTelemetry merge(VectorIndexTypeTelemetry other) {
        if (this == NONE) {
            return other;
        }
        if (other == NONE || other == this) {
            return this;
        }
        return MIXED;
    }
}
