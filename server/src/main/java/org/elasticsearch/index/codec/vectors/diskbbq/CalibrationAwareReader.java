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
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;

/**
 * Implemented by {@link org.apache.lucene.codecs.KnnVectorsReader} implementations that
 * expose calibration-derived values stored in segment metadata.
 */
public interface CalibrationAwareReader {

    /**
     * Returns the calibration-derived oversample factor for the given field, or
     * {@link IvfAutoCalibration#NO_CALIBRATED_OVERSAMPLE} if no calibration data is available.
     */
    float getOversampleFactor(FieldInfo fieldInfo);

    /**
     * Returns whether calibration determined that preconditioning should be applied
     * for the given field. Returns {@code false} if no calibration data is available.
     */
    boolean shouldPrecondition(FieldInfo fieldInfo);

    /**
     * Returns the quantization encoding selected by calibration for the given field,
     * or {@code null} if no calibration data is available.
     */
    ESNextDiskBBQVectorsFormat.QuantEncoding getQuantEncoding(FieldInfo fieldInfo);

}
