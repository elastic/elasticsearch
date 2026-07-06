/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;

/**
 * Resolves a {@link IvfSegmentConfig} on <strong>merge</strong> when {@code auto_calibrate} is enabled.
 * {@link #resolve} requires a non-null {@link MergeState}. Segments with fewer than
 * {@link #MIN_VECTORS_FOR_CALIBRATION} merged vectors get {@code Float.Nan} (no calibrated oversample).
 */
public class IvfAutoCalibration {

    private static final Logger logger = LogManager.getLogger(IvfAutoCalibration.class);

    public static final float NO_CALIBRATED_OVERSAMPLE = Float.NaN;

    public static final int MIN_VECTORS_FOR_CALIBRATION = 10_000;

    /**
     * Minimum fraction of total docs that must agree on a single encoding to skip re-calibration
     * when input segments disagree.
     */
    static final double ENCODING_AGREEMENT_THRESHOLD = 0.8;

    /**
     * Returns an {@link IvfMergeConfigResolver} that runs merge-time auto-calibration for the given cluster size.
     */
    public static IvfMergeConfigResolver mergeConfigResolver(int vectorsPerCluster) {
        return (fieldInfo, mergeState, codecDefault) -> new IvfAutoCalibration().resolve(fieldInfo, mergeState, codecDefault);
    }

    /**
     * For now, just returns the codec default.
     */
    public IvfSegmentConfig resolve(FieldInfo fieldInfo, MergeState mergeState, IvfSegmentConfig codecDefault) throws IOException {
        return codecDefault;
    }

}
