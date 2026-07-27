/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.calibrate;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;

public class CalibrationUtilsTests extends ESTestCase {

    private static final int DIM = 4;

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
        CalibrationUtils.materializeCalibrationQuery(fvv, 0, dim, dim, false, false, preconditioner, true, dst, preconditionScratch);
        assertArrayEquals(preconditioned, dst, 1e-5f);

        CalibrationUtils.materializeCalibrationQuery(fvv, 0, dim, dim, false, false, preconditioner, false, dst, preconditionScratch);
        assertArrayEquals(raw, dst, 1e-5f);
    }

    public void testSampleDataDisjointAndRespectsCaps() throws IOException {
        float[][] data = new float[200][];
        for (int i = 0; i < data.length; i++) {
            data[i] = new float[] { i, i + 1f };
        }
        FloatVectorValues fvv = KMeansFloatVectorValues.build(List.of(data), null, 2);
        CalibrationUtils.SampledData sampled = CalibrationUtils.sampleData(fvv, 32, 64);
        assertEquals(32, sampled.queryOrdinals().length);
        assertEquals(64, sampled.corpusOrdinals().length);
        HashSet<Integer> all = new HashSet<>();
        for (int o : sampled.queryOrdinals()) {
            assertTrue(all.add(o));
        }
        for (int o : sampled.corpusOrdinals()) {
            assertTrue(all.add(o));
        }
    }

    public void testNeedsNeyshaburSrebroLift() {
        assertTrue(CalibrationUtils.needsNeyshaburSrebroLift(VectorSimilarityFunction.DOT_PRODUCT));
        assertTrue(CalibrationUtils.needsNeyshaburSrebroLift(VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT));
        assertFalse(CalibrationUtils.needsNeyshaburSrebroLift(VectorSimilarityFunction.EUCLIDEAN));
        assertFalse(CalibrationUtils.needsNeyshaburSrebroLift(VectorSimilarityFunction.COSINE));
    }

    public void testNeyshaburLiftedSourceAddsLiftDimension() throws IOException {
        float[][] data = { { 1f, 0f }, { 0.5f, 0f } };
        FloatVectorValues base = KMeansFloatVectorValues.build(List.of(data), null, 2);
        double maxNormSq = CalibrationUtils.maxSquaredNormOverCorpusSample(base, new int[] { 0, 1 });
        CalibrationUtils.NeyshaburLiftedSource<FloatVectorValues> lifted = new CalibrationUtils.NeyshaburLiftedSource<>(base, 2, maxNormSq);
        assertEquals(3, lifted.liftedDimension());
        assertEquals(2, lifted.size());

        // explicit scratch read
        float[] scratch = new float[lifted.liftedDimension()];
        lifted.liftedVector(0, scratch);
        float firstLift = scratch[2];
        lifted.liftedVector(1, scratch);
        float secondLift = scratch[2];
        // the shorter vector (smaller norm) gets the larger lift component
        assertThat(secondLift, greaterThan(firstLift));

        // FloatVectorValues bridge produces the same lifted vectors
        FloatVectorValues bridged = lifted.asFloatVectorValues();
        assertEquals(3, bridged.dimension());
        assertEquals(2, bridged.size());
        assertEquals(firstLift, bridged.vectorValue(0)[2], 0f);
        assertEquals(secondLift, bridged.vectorValue(1)[2], 0f);
    }

    private static KnnVectorsReader heapVectorReader(FieldInfo fieldInfo, FloatVectorValues vectors) {
        return new KnnVectorsReader() {
            @Override
            public FloatVectorValues getFloatVectorValues(String field) {
                return field.equals(fieldInfo.name) ? vectors : null;
            }

            @Override
            public org.apache.lucene.index.ByteVectorValues getByteVectorValues(String field) {
                return null;
            }

            @Override
            public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) {}

            @Override
            public void search(
                String field,
                byte[] target,
                org.apache.lucene.search.KnnCollector knnCollector,
                org.apache.lucene.search.AcceptDocs acceptDocs
            ) {}

            @Override
            public Map<String, Long> getOffHeapByteSize(FieldInfo info) {
                return Map.of();
            }

            @Override
            public void checkIntegrity() {}

            @Override
            public void close() {}
        };
    }

    private static MergeState mergeState(KnnVectorsReader[] readers, Bits[] liveDocsBits, SegmentInfo segmentInfo, FieldInfo fieldInfo)
        throws IOException {
        FieldInfos[] fieldInfos = new FieldInfos[readers.length];
        for (int i = 0; i < readers.length; i++) {
            FloatVectorValues vectors = readers[i].getFloatVectorValues(fieldInfo.name);
            fieldInfos[i] = vectors != null ? new FieldInfos(new FieldInfo[] { fieldInfo }) : new FieldInfos(new FieldInfo[0]);
        }
        return new MergeState(
            null,
            segmentInfo,
            null,
            null,
            null,
            null,
            null,
            fieldInfos,
            liveDocsBits,
            null,
            null,
            readers,
            null,
            null,
            null,
            false,
            null
        );
    }

    private static Bits liveDocs(int length) {
        return new Bits() {
            @Override
            public boolean get(int index) {
                return true;
            }

            @Override
            public int length() {
                return length;
            }
        };
    }

    private static SegmentInfo backgroundSegmentInfo(Directory dir) throws IOException {
        return new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            "bg",
            1000,
            false,
            false,
            Codec.getDefault(),
            Collections.emptyMap(),
            StringHelper.randomId(),
            new HashMap<>(),
            null
        );
    }

    private static FieldInfo vectorFieldInfo(String name) {
        return new FieldInfo(
            name,
            0,
            false,
            false,
            false,
            IndexOptions.NONE,
            DocValuesType.NONE,
            org.apache.lucene.index.DocValuesSkipIndexType.NONE,
            -1,
            Map.of(),
            0,
            0,
            0,
            DIM,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }
}
