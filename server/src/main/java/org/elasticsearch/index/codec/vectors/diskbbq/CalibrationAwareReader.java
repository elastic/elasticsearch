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
import org.elasticsearch.core.Nullable;

/**
 * Implemented by {@link org.apache.lucene.codecs.KnnVectorsReader} implementations that
 * expose calibration-derived parameters stored in segment metadata.
 */
public interface CalibrationAwareReader {

    /**
     * Returns the calibration parameters for the given field, or {@code null} if this segment
     * was not auto-calibrated for that field.
     */
    @Nullable
    SegmentCalibrationParameters getCalibrationParameters(FieldInfo fieldInfo);

}
