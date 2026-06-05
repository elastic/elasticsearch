/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class MergeCalibrationSampleVectorsTests extends ESTestCase {

    private static final int DIM = 4;
    private static final String FIELD = "vec";

    public void testCountMergedVectorsSumsSegments() throws IOException {
        FieldInfo fieldInfo = vectorFieldInfo(FIELD);
        try (Directory dir = newDirectory()) {
            MergeState mergeState = mergeState(
                dir,
                new KnnVectorsReader[] { heapReader(fieldInfo, heapVectors(100)), heapReader(fieldInfo, heapVectors(250)) },
                fieldInfo
            );
            assertThat(MergeCalibrationSampleVectors.countMergedVectors(fieldInfo, mergeState), equalTo(350));
        }
    }

    public void testBuildTakesPerSegmentTargetFromEqualSegments() throws IOException {
        FieldInfo fieldInfo = vectorFieldInfo(FIELD);
        int globalCap = 20;
        int perSegmentTarget = globalCap / 2;
        try (Directory dir = newDirectory()) {
            MergeState mergeState = mergeState(
                dir,
                new KnnVectorsReader[] { heapReader(fieldInfo, heapVectors(100)), heapReader(fieldInfo, heapVectors(100)) },
                fieldInfo
            );
            FloatVectorValues sample = MergeCalibrationSampleVectors.build(fieldInfo, mergeState, globalCap);
            assertThat(sample.size(), equalTo(globalCap));
            assertThat(sample.vectorValue(0)[0], equalTo(0f));
            assertThat(sample.vectorValue(perSegmentTarget)[0], equalTo(0f));
            assertThat(sample.vectorValue(perSegmentTarget - 1)[0], equalTo((float) (perSegmentTarget - 1)));
        }
    }

    public void testBuildBackfillsWhenSegmentIsShort() throws IOException {
        FieldInfo fieldInfo = vectorFieldInfo(FIELD);
        int globalCap = 10;
        try (Directory dir = newDirectory()) {
            MergeState mergeState = mergeState(
                dir,
                new KnnVectorsReader[] { heapReader(fieldInfo, heapVectors(2)), heapReader(fieldInfo, heapVectors(100)) },
                fieldInfo
            );
            FloatVectorValues sample = MergeCalibrationSampleVectors.build(fieldInfo, mergeState, globalCap);
            assertThat(sample.size(), equalTo(globalCap));
            assertThat(sample.vectorValue(0)[0], equalTo(0f));
            assertThat(sample.vectorValue(1)[0], equalTo(1f));
            assertThat(sample.vectorValue(2)[0], equalTo(0f));
            assertThat(sample.vectorValue(9)[0], equalTo(7f));
        }
    }

    public void testBuildSkipsSegmentsMissingField() throws IOException {
        FieldInfo fieldInfo = vectorFieldInfo(FIELD);
        FieldInfo otherField = vectorFieldInfo("other");
        try (Directory dir = newDirectory()) {
            MergeState mergeState = mergeState(
                dir,
                new KnnVectorsReader[] { heapReader(fieldInfo, heapVectors(5)), heapReader(otherField, heapVectors(50)) },
                fieldInfo
            );
            FloatVectorValues sample = MergeCalibrationSampleVectors.build(fieldInfo, mergeState, 10);
            assertThat(sample.size(), equalTo(5));
        }
    }

    private static List<float[]> heapVectors(int count) {
        List<float[]> vectors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            float[] vector = new float[DIM];
            vector[0] = i;
            vectors.add(vector);
        }
        return vectors;
    }

    private static KnnVectorsReader heapReader(FieldInfo fieldInfo, List<float[]> vectors) {
        FloatVectorValues fvv = KMeansFloatVectorValues.build(vectors, null, DIM);
        return new KnnVectorsReader() {
            @Override
            public FloatVectorValues getFloatVectorValues(String field) {
                return field.equals(fieldInfo.name) ? fvv : null;
            }

            @Override
            public org.apache.lucene.index.ByteVectorValues getByteVectorValues(String field) {
                return null;
            }

            @Override
            public void search(
                String field,
                float[] target,
                org.apache.lucene.search.KnnCollector knnCollector,
                org.apache.lucene.search.AcceptDocs acceptDocs
            ) {}

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

    private static MergeState mergeState(Directory dir, KnnVectorsReader[] readers, FieldInfo fieldInfo) throws IOException {
        FieldInfos[] fieldInfos = new FieldInfos[readers.length];
        for (int i = 0; i < readers.length; i++) {
            FloatVectorValues vectors = readers[i].getFloatVectorValues(fieldInfo.name);
            if (vectors != null) {
                fieldInfos[i] = new FieldInfos(new FieldInfo[] { fieldInfo });
            } else {
                fieldInfos[i] = new FieldInfos(new FieldInfo[0]);
            }
        }
        return new MergeState(
            null,
            backgroundSegmentInfo(dir),
            null,
            null,
            null,
            null,
            null,
            fieldInfos,
            new Bits[readers.length],
            null,
            null,
            readers,
            null,
            null,
            null,
            false
        );
    }

    private static SegmentInfo backgroundSegmentInfo(Directory dir) {
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
            org.apache.lucene.index.DocValuesType.NONE,
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
