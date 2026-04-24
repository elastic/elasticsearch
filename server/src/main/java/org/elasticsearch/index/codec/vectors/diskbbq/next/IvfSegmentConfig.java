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
 * Per-segment (per-field) IVF options for indexing. At query time only {@code centroidOversamplingFactor}
 * is read from {@code mivf}; whether the query is preconditioned follows the same on-disk preconditioner
 * as mapping-driven indexing.
 * When {@link #centroidOversamplingFactor} is {@link Float#isNaN}, the reader uses the historical
 * derived factor {@code (float) numCentroids / (2 * numParents)} when {@code numParents} &gt; 0.
 */
public record IvfSegmentConfig(
    ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding,
    boolean usePrecondition,
    float centroidOversamplingFactor
) {

    public static IvfSegmentConfig fromCodecDefaults(ESNextDiskBBQVectorsFormat.QuantEncoding quantEncoding, boolean doPrecondition) {
        return new IvfSegmentConfig(quantEncoding, doPrecondition, Float.NaN);
    }
}
