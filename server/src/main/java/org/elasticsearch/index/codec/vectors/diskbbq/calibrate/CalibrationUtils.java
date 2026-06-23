/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.calibrate;

import org.apache.lucene.index.FloatVectorValues;
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Objects;

/**
 * Shared utilities for DiskBBQ merge-time calibration.
 */
public final class CalibrationUtils {

    private CalibrationUtils() {}

    /**
     * Buffer length for calibration query scratch arrays: {@code baseDim}, or {@code baseDim + 1}
     * when Neyshabur lift applies.
     */
    public static int calibrationQueryDimension(int baseDim, boolean neyshabur) {
        return neyshabur ? baseDim + 1 : baseDim;
    }

    /**
     * Materializes one calibration query from {@code querySource} at {@code queryOrdinal} into
     * {@code dst}. {@code dst.length} must be at least {@code dimWork}. When
     * {@code usePreconditioned} is true and {@code preconditioner} is non-null, applies it using
     * {@code preconditionScratch} (length at least {@code dimWork}).
     */
    public static void materializeCalibrationQuery(
        FloatVectorValues querySource,
        int queryOrdinal,
        int baseDim,
        int dimWork,
        boolean cosine,
        boolean neyshabur,
        Preconditioner preconditioner,
        boolean usePreconditioned,
        float[] dst,
        float[] preconditionScratch
    ) throws IOException {
        float[] raw = querySource.vectorValue(queryOrdinal);
        System.arraycopy(raw, 0, dst, 0, baseDim);
        if (cosine) {
            ESVectorUtil.l2Normalize(dst, baseDim);
        }
        if (neyshabur) {
            dst[baseDim] = 0f;
        }
        if (usePreconditioned && preconditioner != null) {
            Objects.requireNonNull(preconditionScratch, "preconditionScratch");
            preconditioner.applyTransform(dst, preconditionScratch);
            System.arraycopy(preconditionScratch, 0, dst, 0, dimWork);
        }
    }
}
