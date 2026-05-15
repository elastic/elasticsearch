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
 * Selects i) a concrete {@link ESNextDiskBBQVectorsFormat.QuantEncoding}, ii) the required oversampling ratio, and
 * iii) whether preconditioning should be used, for IVF/diskbbq when automatic calibration is enabled on
 * <strong>merge</strong> (after merged vectors and centroid structure exist). Lucene flush uses codec/mapping defaults
 * instead of calling this path.
 */
@FunctionalInterface
public interface AutoCalibrationSelector {

    /**
     * Default oversample used when the segment is too small for calibration.
     */
    float DEFAULT_CALIBRATED_OVERSAMPLE = 3f;

    /**
     * No calibrated oversample; indicates the segment has no calibration-derived oversample.
     */
    float NO_CALIBRATED_OVERSAMPLE = DEFAULT_CALIBRATED_OVERSAMPLE;

    /**
     * Choose the quantization encoding, oversample, and whether to precondition for the merged segment.
     *
     * @param fieldInfo          field metadata (dimension, similarity)
     * @param floatVectorValues  merged vectors (typically after merge-time preconditioning)
     * @param globalCentroid    global centroid
     * @param mergeState         Lucene merge state (required; bounded force-merge merges are detected from diagnostics)
     * @return calibration      result containing the encoding and oversample (never null)
     */
    IvfSegmentConfig select(FieldInfo fieldInfo, FloatVectorValues floatVectorValues, float[] globalCentroid, MergeState mergeState);
}
