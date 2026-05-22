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

    public static IvfSegmentConfig fromCodecDefaults(ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding, boolean doPrecondition) {
        return new IvfSegmentConfig(quantEncoding, doPrecondition, Float.NaN);
    }
}
