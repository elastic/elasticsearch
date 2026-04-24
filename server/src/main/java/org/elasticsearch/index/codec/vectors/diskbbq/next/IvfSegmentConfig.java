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
 * Per-segment (per-field) IVF bundle persisted in {@code mivf} for DiskBBQ. It has three
 * parts: {@link #quantEncoding()} for scalar quant used when indexing doc vectors,
 * {@link #usePrecondition()} for whether a preconditioner is written and used on flush/merge and on the
 * reader, and {@link #rescoreOversample()} for kNN rescore candidate expansion (v2+ {@code mivf}),
 * read with query and mapping in {@link org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType}.
 *
 * The effective config from flush/merge is written to stay consistent with the quantization and
 * preconditioning data stored for the segment. The mapping flag {@code persist_ivf_segment_config}
 * (default {@code false}) controls whether {@link org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsWriter}
 * may apply calibration and persist the full bundle. When the flag is {@code false}, writers use only
 * codec/mapping defaults (and write a non-finite rescore slot in {@code mivf}).
 * Search-time scoring for quant and preconditioning continues to follow the on-disk {@code mivf} and
 * reader, as for uncalibrated segments. Only the kNN <em>rescore oversample</em> read from {@code mivf}
 * is additionally gated in {@link org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType}
 * when the flag is {@code false}.
 * When the stored rescore is not finite (e.g. {@code NaN}), the index leg of oversample is skipped;
 * query and mapping rescore then apply in the usual order.
 */
public record IvfSegmentConfig(ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding, boolean usePrecondition, float rescoreOversample) {

    public static IvfSegmentConfig fromCodecDefaults(ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding, boolean doPrecondition) {
        return new IvfSegmentConfig(quantEncoding, doPrecondition, Float.NaN);
    }
}
