/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.MergeState;

/**
 * Default implementation of {@link AutoCalibrationSelector}.
 * Returns a fixed encoding; can be replaced with a more advanced implementation.
 */
public final class NoOpAutomaticCalibrationSelector implements AutoCalibrationSelector {

    public static final NoOpAutomaticCalibrationSelector INSTANCE = new NoOpAutomaticCalibrationSelector();

    private NoOpAutomaticCalibrationSelector() {}

    @Override
    public IvfSegmentConfig select(
        FieldInfo fieldInfo,
        FloatVectorValues floatVectorValues,
        float[] globalCentroid,
        MergeState mergeState
    ) {
        return new IvfSegmentConfig(ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY, false, NO_CALIBRATED_OVERSAMPLE);
    }
}
