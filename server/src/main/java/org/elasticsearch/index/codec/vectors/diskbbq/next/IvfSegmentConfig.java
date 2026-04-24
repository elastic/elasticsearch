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
 * Per-segment (per-field) IVF options for indexing. The persisted {@code rescoreOversample} is
 * read at query time to participate in the same place as {@code rescore_vector.oversample} (see
 * kNN rescore in {@link org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType}).
 * It is not used for the centroid-visit budget; that remains {@code (float) numCentroids / (2 * numParents)}.
 * <p>
 * When the stored value is not finite (e.g. {@code NaN}), the index leg of oversample is skipped;
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
