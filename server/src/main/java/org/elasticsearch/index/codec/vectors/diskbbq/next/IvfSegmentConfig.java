/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

/**
 * Per-segment (per-field) IVF bundle persisted in {@code mivf} for <em>ESNext</em> DiskBBQ. It has three
 * parts: {@link #quantEncoding()} for scalar quant used when indexing vectors into clusters,
 * {@link #usePrecondition()} for whether a preconditioner is written and used on flush/merge and on the
 * reader, and {@link #rescoreOversample()} for kNN rescore candidate expansion (v2+ {@code mivf}),
 * read with query and mapping in {@link org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType}.
 * Rescore is not used for the centroid-visit budget; that remains {@code (float) numCentroids / (2 * numParents)}.
 * <p>
 * For {@code bbq_disk} indices, the mapping flag {@code persist_ivf_segment_config} (default {@code false})
 * controls whether flush/merge may calibrate and persist the full bundle and whether search uses on-disk
 * values for all three. When the flag is {@code false}, writers only persist mapping/codec defaults (and
 * write a non-finite rescore slot), and kNN search uses the mapping for quant, precondition, and rescore
 * instead of {@code mivf}, even in older segments.
 * <p>
 * When the stored rescore is not finite (e.g. {@code NaN}), the index leg of oversample is skipped;
 * query and mapping rescore then apply in the usual order.
 */
public record IvfSegmentConfig(
    ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding,
    boolean usePrecondition,
    float rescoreOversample
) {

    public static IvfSegmentConfig fromCodecDefaults(ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding, boolean doPrecondition) {
        return new IvfSegmentConfig(quantEncoding, doPrecondition, Float.NaN);
    }
}
