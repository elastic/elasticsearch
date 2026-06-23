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

public class CalibrationQueriesTests extends ESTestCase {

    public void testSizeAndDimension() {
        float[][] corpus = { { 1f, 0f }, { 0f, 1f }, { 1f, 1f } };
        FloatVectorValues fvv = KMeansFloatVectorValues.build(List.of(corpus), null, 2);
        int[] queryOrdinals = { 0, 2 };
        CalibrationQueries queries = new CalibrationQueries(fvv, queryOrdinals, 2, false, false, null, 2);

        assertEquals(2, queries.size());
        assertEquals(2, queries.dimension());
    }

    public void testCopyQueryReadsVectorsByOrdinal() throws IOException {
        float[][] corpus = { { 1f, 0f, 0f }, { 0f, 1f, 0f }, { 0f, 0f, 1f } };
        FloatVectorValues fvv = KMeansFloatVectorValues.build(List.of(corpus), null, 3);
        int[] queryOrdinals = { 2, 0 };
        CalibrationQueries queries = new CalibrationQueries(fvv, queryOrdinals, 3, false, false, null, 3);

        float[] dst = new float[3];
        queries.copyQuery(0, false, dst);
        assertArrayEquals(new float[] { 0f, 0f, 1f }, dst, 1e-5f);

        queries.copyQuery(1, false, dst);
        assertArrayEquals(new float[] { 1f, 0f, 0f }, dst, 1e-5f);
    }

    public void testCopyQueryNormalizesWhenCosine() throws IOException {
        float[][] corpus = { { 3f, 4f } };
        FloatVectorValues fvv = KMeansFloatVectorValues.build(List.of(corpus), null, 2);
        CalibrationQueries queries = new CalibrationQueries(fvv, new int[] { 0 }, 2, true, false, null, 2);

        float[] dst = new float[2];
        queries.copyQuery(0, false, dst);

        assertThat((double) ESVectorUtil.dotProduct(dst, dst), closeTo(1.0, 1e-5));
        assertThat((double) dst[0], closeTo(0.6, 1e-5));
        assertThat((double) dst[1], closeTo(0.8, 1e-5));
    }

    public void testCopyQueryAppendsNeyshaburLiftDimension() throws IOException {
        int baseDim = 4;
        float[][] corpus = { { 1f, 2f, 3f, 4f } };
        FloatVectorValues fvv = KMeansFloatVectorValues.build(List.of(corpus), null, baseDim);
        CalibrationQueries queries = new CalibrationQueries(fvv, new int[] { 0 }, baseDim, false, true, null, baseDim + 1);

        assertEquals(baseDim + 1, queries.dimension());

        float[] dst = new float[baseDim + 1];
        queries.copyQuery(0, false, dst);

        assertArrayEquals(new float[] { 1f, 2f, 3f, 4f, 0f }, dst, 1e-5f);
    }

    public void testCopyQueryAppliesPreconditionerWhenRequested() throws IOException {
        int dim = 8;
        int blockDim = 4;
        float[][] corpus = { { 1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f } };
        FloatVectorValues fvv = KMeansFloatVectorValues.build(List.of(corpus), null, dim);
        Preconditioner preconditioner = Preconditioner.createPreconditioner(dim, blockDim);
        CalibrationQueries queries = new CalibrationQueries(fvv, new int[] { 0 }, dim, false, false, preconditioner, dim);

        float[] raw = corpus[0].clone();
        float[] preconditioned = new float[dim];
        preconditioner.applyTransform(raw, preconditioned);

        float[] dst = new float[dim];
        queries.copyQuery(0, true, dst);
        assertArrayEquals(preconditioned, dst, 1e-5f);

        queries.copyQuery(0, false, dst);
        assertArrayEquals(raw, dst, 1e-5f);
    }
}
