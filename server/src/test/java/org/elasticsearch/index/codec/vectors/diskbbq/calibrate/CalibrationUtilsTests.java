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
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.closeTo;

public class CalibrationUtilsTests extends ESTestCase {

    public void testCalibrationQueryDimension() {
        assertEquals(8, CalibrationUtils.calibrationQueryDimension(8, false));
        assertEquals(9, CalibrationUtils.calibrationQueryDimension(8, true));
    }

    public void testMaterializeCalibrationQueryReadsVectorsByOrdinal() throws IOException {
        float[][] corpus = { { 1f, 0f, 0f }, { 0f, 1f, 0f }, { 0f, 0f, 1f } };
        FloatVectorValues fvv = KMeansFloatVectorValues.build(List.of(corpus), null, 3);
        int[] queryOrdinals = { 2, 0 };

        float[] dst = new float[3];
        CalibrationUtils.materializeCalibrationQuery(fvv, queryOrdinals[0], 3, 3, false, false, null, false, dst, null);
        assertArrayEquals(new float[] { 0f, 0f, 1f }, dst, 1e-5f);

        CalibrationUtils.materializeCalibrationQuery(fvv, queryOrdinals[1], 3, 3, false, false, null, false, dst, null);
        assertArrayEquals(new float[] { 1f, 0f, 0f }, dst, 1e-5f);
    }

    public void testMaterializeCalibrationQueryNormalizesWhenCosine() throws IOException {
        float[][] corpus = { { 3f, 4f } };
        FloatVectorValues fvv = KMeansFloatVectorValues.build(List.of(corpus), null, 2);

        float[] dst = new float[2];
        CalibrationUtils.materializeCalibrationQuery(fvv, 0, 2, 2, true, false, null, false, dst, null);

        assertThat((double) ESVectorUtil.dotProduct(dst, dst), closeTo(1.0, 1e-5));
        assertThat((double) dst[0], closeTo(0.6, 1e-5));
        assertThat((double) dst[1], closeTo(0.8, 1e-5));
    }

    public void testMaterializeCalibrationQueryAppendsNeyshaburLiftDimension() throws IOException {
        int baseDim = 4;
        float[][] corpus = { { 1f, 2f, 3f, 4f } };
        FloatVectorValues fvv = KMeansFloatVectorValues.build(List.of(corpus), null, baseDim);

        float[] dst = new float[baseDim + 1];
        CalibrationUtils.materializeCalibrationQuery(fvv, 0, baseDim, baseDim + 1, false, true, null, false, dst, null);

        assertArrayEquals(new float[] { 1f, 2f, 3f, 4f, 0f }, dst, 1e-5f);
    }

    public void testMaterializeCalibrationQueryAppliesPreconditionerWhenRequested() throws IOException {
        int dim = 8;
        int blockDim = 4;
        float[][] corpus = { { 1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f } };
        FloatVectorValues fvv = KMeansFloatVectorValues.build(List.of(corpus), null, dim);
        Preconditioner preconditioner = Preconditioner.createPreconditioner(dim, blockDim);

        float[] raw = corpus[0].clone();
        float[] preconditioned = new float[dim];
        preconditioner.applyTransform(raw, preconditioned);

        float[] dst = new float[dim];
        float[] preconditionScratch = new float[dim];
        CalibrationUtils.materializeCalibrationQuery(
            fvv,
            0,
            dim,
            dim,
            false,
            false,
            preconditioner,
            true,
            dst,
            preconditionScratch
        );
        assertArrayEquals(preconditioned, dst, 1e-5f);

        CalibrationUtils.materializeCalibrationQuery(fvv, 0, dim, dim, false, false, preconditioner, false, dst, preconditionScratch);
        assertArrayEquals(raw, dst, 1e-5f);
    }
}
