/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;

/**
 * Per-segment (per-field) IVF configuration persisted in {@code mivf}. It has three
 * parts: {@link #quantEncoding()} for scalar quant used when indexing doc vectors,
 * {@link #usePrecondition()} for whether a preconditioner is written and used on flush/merge and on the
 * reader, and {@link #rescoreOversample()} for kNN rescore candidate expansion,
 * read with query.
 * The effective config from flush/merge is written to stay consistent with the quantization and
 * preconditioning data stored for the segment.
 * Search-time scoring for quant and preconditioning continues to follow the on-disk {@code mivf} and
 * reader.
 * When the stored rescore is not finite (e.g. {@code NaN}), query and mapping rescore then apply in the usual order.
 */
public record IvfSegmentConfig(ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding, boolean usePrecondition, float rescoreOversample) {

    public static final IvfSegmentConfig NONE = new IvfSegmentConfig(
        ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY,
        false,
        Float.NaN
    );

    public static IvfSegmentConfig fromCodecDefaults(ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding, boolean doPrecondition) {
        return new IvfSegmentConfig(quantEncoding, doPrecondition, Float.NaN);
    }

    /**
     * Resolves oversample for search: query override, else finite persisted value, else mapping default.
     */
    public static float effectiveRescoreOversample(float persisted, Float queryOverride, float mappingDefault) {
        if (queryOverride != null) {
            return queryOverride;
        }
        if (Float.isFinite(persisted)) {
            return persisted;
        }
        return mappingDefault;
    }

    /**
     * Returns a copy of {@code raw} with {@link #rescoreOversample()} set to the effective value.
     */
    public static IvfSegmentConfig withEffectiveRescoreOversample(IvfSegmentConfig raw, Float queryOverride, float mappingDefault) {
        float effective = effectiveRescoreOversample(raw.rescoreOversample(), queryOverride, mappingDefault);
        return new IvfSegmentConfig(raw.quantEncoding(), raw.usePrecondition(), effective);
    }

    /** Per-leaf IVF collector size (includes 2x factor for overspill duplicates). */
    public static int leafCollectorBudget(int resultK, float segmentOversample) {
        return Math.round(2f * resultK * Math.max(1, segmentOversample));
    }

    /** Shard-level merge cap across segments after approximate search. */
    public static int shardMergeBudget(int resultK, float maxSegmentOversample) {
        return (int) Math.ceil(resultK * Math.max(1, maxSegmentOversample));
    }
}
