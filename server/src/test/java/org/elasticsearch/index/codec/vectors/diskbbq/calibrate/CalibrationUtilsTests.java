/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public License
 * v3.0 only", or the "Server Side Public License, v 1".
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
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;

public class CalibrationUtilsTests extends ESTestCase {

    private static final int DIM = 4;

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

    public void testSampleDataOnConcatenatedView() throws IOException {
        FloatVectorValues seg0 = KMeansFloatVectorValues.build(
            List.of(new float[] { 1f, 0f, 0f, 0f }, new float[] { 2f, 0f, 0f, 0f }),
            null,
            DIM
        );
        FloatVectorValues seg1 = KMeansFloatVectorValues.build(
            List.of(new float[] { 3f, 0f, 0f, 0f }, new float[] { 4f, 0f, 0f, 0f }, new float[] { 5f, 0f, 0f, 0f }),
            null,
            DIM
        );
        FieldInfo fieldInfo = vectorFieldInfo("vec");
        try (Directory dir = newDirectory()) {
            FloatVectorValues concatenated = CalibrationUtils.build(
                fieldInfo,
                mergeState(
                    new KnnVectorsReader[] { heapVectorReader(fieldInfo, seg0), heapVectorReader(fieldInfo, seg1) },
                    new Bits[] { liveDocs(1), liveDocs(2) },
                    backgroundSegmentInfo(dir),
                    fieldInfo
                )
            );
            assertEquals(5, concatenated.size());

            CalibrationUtils.SampledData sampled = CalibrationUtils.sampleData(concatenated, 1, 2);
            assertEquals(1, sampled.queryOrdinals().length);
            assertEquals(2, sampled.corpusOrdinals().length);
            HashSet<Integer> all = new HashSet<>();
            for (int o : sampled.queryOrdinals()) {
                assertTrue(all.add(o));
            }
            for (int o : sampled.corpusOrdinals()) {
                assertTrue(all.add(o));
            }
            assertTrue(sampled.queryOrdinals()[0] >= 0 && sampled.queryOrdinals()[0] < 5);
            assertTrue(sampled.corpusOrdinals()[0] >= 0 && sampled.corpusOrdinals()[0] < 5);
            assertTrue(sampled.corpusOrdinals()[1] >= 0 && sampled.corpusOrdinals()[1] < 5);
        }
    }

    public void testConcatenatedViewDispatchesByGlobalOrdinal() throws IOException {
        FloatVectorValues seg0 = KMeansFloatVectorValues.build(List.of(new float[] { 1f, 2f, 3f, 4f }), null, DIM);
        FloatVectorValues seg1 = KMeansFloatVectorValues.build(
            List.of(new float[] { 5f, 6f, 7f, 8f }, new float[] { 9f, 10f, 11f, 12f }),
            null,
            DIM
        );
        FieldInfo fieldInfo = vectorFieldInfo("vec");
        try (Directory dir = newDirectory()) {
            FloatVectorValues concatenated = CalibrationUtils.build(
                fieldInfo,
                mergeState(
                    new KnnVectorsReader[] { heapVectorReader(fieldInfo, seg0), heapVectorReader(fieldInfo, seg1) },
                    new Bits[] { liveDocs(1), liveDocs(2) },
                    backgroundSegmentInfo(dir),
                    fieldInfo
                )
            );

            assertEquals(3, concatenated.size());
            assertArrayEquals(new float[] { 1f, 2f, 3f, 4f }, concatenated.vectorValue(0), 0f);
            assertArrayEquals(new float[] { 5f, 6f, 7f, 8f }, concatenated.vectorValue(1), 0f);
            assertArrayEquals(new float[] { 9f, 10f, 11f, 12f }, concatenated.vectorValue(2), 0f);
        }
    }

    public void testBuildFromSegmentReadersNoHeapCopy() throws IOException {
        FloatVectorValues seg0 = KMeansFloatVectorValues.build(List.of(new float[] { 1f, 0f, 0f, 0f }), null, DIM);
        FloatVectorValues seg1 = KMeansFloatVectorValues.build(
            List.of(new float[] { 2f, 0f, 0f, 0f }, new float[] { 3f, 0f, 0f, 0f }),
            null,
            DIM
        );
        FieldInfo fieldInfo = vectorFieldInfo("vec");
        try (Directory dir = newDirectory()) {
            MergeState mergeState = mergeState(
                new KnnVectorsReader[] { heapVectorReader(fieldInfo, seg0), heapVectorReader(fieldInfo, seg1) },
                new Bits[] { liveDocs(1), liveDocs(2) },
                backgroundSegmentInfo(dir),
                fieldInfo
            );

            FloatVectorValues built = CalibrationUtils.build(fieldInfo, mergeState);
            assertNotSame(seg0, built);
            assertNotSame(seg1, built);
            assertEquals(3, built.size());
            assertArrayEquals(new float[] { 1f, 0f, 0f, 0f }, built.vectorValue(0), 0f);
            assertArrayEquals(new float[] { 2f, 0f, 0f, 0f }, built.vectorValue(1), 0f);
            assertArrayEquals(new float[] { 3f, 0f, 0f, 0f }, built.vectorValue(2), 0f);
        }
    }

    public void testBuildSingleSegmentReturnsDelegate() throws IOException {
        FloatVectorValues seg0 = KMeansFloatVectorValues.build(List.of(new float[] { 1f, 2f, 3f, 4f }), null, DIM);
        FieldInfo fieldInfo = vectorFieldInfo("vec");
        try (Directory dir = newDirectory()) {
            MergeState mergeState = mergeState(
                new KnnVectorsReader[] { heapVectorReader(fieldInfo, seg0) },
                new Bits[] { liveDocs(1) },
                backgroundSegmentInfo(dir),
                fieldInfo
            );
            assertSame(seg0, CalibrationUtils.build(fieldInfo, mergeState));
        }
    }

    public void testNeedsNeyshaburSrebroLift() {
        assertTrue(CalibrationUtils.needsNeyshaburSrebroLift(VectorSimilarityFunction.DOT_PRODUCT));
        assertTrue(CalibrationUtils.needsNeyshaburSrebroLift(VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT));
        assertFalse(CalibrationUtils.needsNeyshaburSrebroLift(VectorSimilarityFunction.EUCLIDEAN));
        assertFalse(CalibrationUtils.needsNeyshaburSrebroLift(VectorSimilarityFunction.COSINE));
    }

    public void testNeyshaburCorpusFloatVectorValuesAddsLiftDimension() throws IOException {
        float[][] data = { { 1f, 0f }, { 0.5f, 0f } };
        FloatVectorValues base = KMeansFloatVectorValues.build(List.of(data), null, 2);
        double maxNormSq = CalibrationUtils.maxSquaredNormOverCorpusSample(base, new int[] { 0, 1 });
        CalibrationUtils.NeyshaburCorpusFloatVectorValues lifted = new CalibrationUtils.NeyshaburCorpusFloatVectorValues(
            base,
            2,
            maxNormSq
        );
        assertEquals(3, lifted.dimension());
        float firstLift = lifted.vectorValue(0)[2];
        float secondLift = lifted.vectorValue(1)[2];
        assertThat(secondLift, greaterThan(firstLift));
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
            false
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
