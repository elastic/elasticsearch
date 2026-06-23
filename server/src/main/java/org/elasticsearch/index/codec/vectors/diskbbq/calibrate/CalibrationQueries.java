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
 * Late-materialized calibration queries: copies one query at a time from {@link FloatVectorValues}
 * via sample ordinals into a caller-provided buffer, optionally applying cosine normalization,
 * Neyshabur lift, and random orthogonal preconditioning.
 */
public final class CalibrationQueries {

    private final FloatVectorValues baseFvv;
    private final int[] queryOrdinals;
    private final int baseDim;
    private final int dimWork;
    private final boolean cosine;
    private final boolean neyshabur;
    private final Preconditioner preconditioner;
    private final float[] tmpPre;

    public CalibrationQueries(
        FloatVectorValues baseFvv,
        int[] queryOrdinals,
        int baseDim,
        boolean cosine,
        boolean neyshabur,
        Preconditioner preconditioner,
        int dimWork
    ) {
        this.baseFvv = Objects.requireNonNull(baseFvv);
        this.queryOrdinals = Objects.requireNonNull(queryOrdinals);
        this.baseDim = baseDim;
        this.cosine = cosine;
        this.neyshabur = neyshabur;
        this.preconditioner = preconditioner;
        this.dimWork = dimWork;
        this.tmpPre = preconditioner != null ? new float[dimWork] : null;
    }

    public int size() {
        return queryOrdinals.length;
    }

    public int dimension() {
        return dimWork;
    }

    /**
     * Writes query {@code index} into {@code dst}. {@code dst.length} must be at least {@link #dimension()}.
     * When {@code usePreconditioned} is true and a {@link Preconditioner} was configured, applies it
     * (reference calibration orthogonal branch).
     */
    public void copyQuery(int index, boolean usePreconditioned, float[] dst) throws IOException {
        float[] raw = baseFvv.vectorValue(queryOrdinals[index]);
        System.arraycopy(raw, 0, dst, 0, baseDim);
        if (cosine) {
            ESVectorUtil.l2Normalize(dst, baseDim);
        }
        if (neyshabur) {
            dst[baseDim] = 0f;
        }
        if (usePreconditioned && preconditioner != null) {
            Objects.requireNonNull(tmpPre, "tmpPre");
            preconditioner.applyTransform(dst, tmpPre);
            System.arraycopy(tmpPre, 0, dst, 0, dimWork);
        }
    }
}
