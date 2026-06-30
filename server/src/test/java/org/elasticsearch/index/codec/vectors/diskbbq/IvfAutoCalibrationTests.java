/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
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
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextRescoreOversampleTestFixture;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.elasticsearch.index.codec.vectors.diskbbq.IvfAutoCalibration.NO_CALIBRATED_OVERSAMPLE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for {@link IvfAutoCalibration} merge decision logic.
 */
public class IvfAutoCalibrationTests extends ESTestCase {

    private static final int DIM = 64;
    private static final int VPC = 128;

    private static final IvfSegmentConfig CODEC_DEFAULT = IvfSegmentConfig.fromCodecDefaults(
        ESNextDiskBBQVectorsFormat.CentroidIndexFormat.FLAT,
        ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY,
        false
    );

    public void testSelectBelowMinVectorsReturnsDefaultOversample() throws IOException {
        IvfAutoCalibration selector = new IvfAutoCalibration();
        FieldInfo fieldInfo = vectorFieldInfo("f");
        try (Directory dir = newDirectory()) {
            MergeState mergeState = mergeStateWithVectorCount(dir, fieldInfo, 500);

            IvfSegmentConfig config = selector.resolve(fieldInfo, mergeState, CODEC_DEFAULT);

            assertThat(config.quantEncoding(), is(ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY));
            assertFalse(config.usePrecondition());
            assertThat(config.rescoreOversample(), equalTo(CODEC_DEFAULT.rescoreOversample()));
        }
    }

    public void testSelectBoundedForceMergeUsesCodecDefault() throws IOException {
        IvfAutoCalibration selector = new IvfAutoCalibration();
        FieldInfo fieldInfo = vectorFieldInfo("f");
        IvfSegmentConfig codecDefault = IvfSegmentConfig.fromCodecDefaults(
            ESNextDiskBBQVectorsFormat.CentroidIndexFormat.FLAT,
            ESNextDiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY,
            true
        );
        try (Directory dir = newDirectory()) {
            MergeState mergeState = mergeStateWithVectorCount(dir, fieldInfo, IvfAutoCalibration.MIN_VECTORS_FOR_CALIBRATION);

            IvfSegmentConfig config = selector.resolve(fieldInfo, mergeState, codecDefault);

            assertThat(config, equalTo(codecDefault));
        }
    }

    public void testSelectBackgroundMergeUsesCodecDefaultWhenReuseFails() throws IOException {
        IvfAutoCalibration selector = new IvfAutoCalibration();
        FieldInfo fieldInfo = vectorFieldInfo("f");
        try (Directory dir = newDirectory()) {
            MergeState mergeState = mergeStateWithVectorCount(dir, fieldInfo, IvfAutoCalibration.MIN_VECTORS_FOR_CALIBRATION);

            IvfSegmentConfig config = selector.resolve(fieldInfo, mergeState, CODEC_DEFAULT);

            assertThat(config, equalTo(CODEC_DEFAULT));
        }
    }

    public void testProductionMergeResolverPersistsCodecDefaultOnForceMerge() throws IOException {
        Random rnd = random();
        int vectorsPerSegment = IvfAutoCalibration.MIN_VECTORS_FOR_CALIBRATION / 2 + 100;
        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildForceMergedWithDisagreeingFlushCalibration(
                    dir,
                    rnd,
                    8,
                    vectorsPerSegment,
                    VPC
                )
            ) {
                IvfSegmentConfig persisted = ESNextRescoreOversampleTestFixture.readPersistedSegmentConfig(
                    reader.leaves().getFirst().reader()
                );
                assertNotNull(persisted);
                assertThat(persisted.quantEncoding(), is(ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY));
                assertFalse(persisted.usePrecondition());
                assertThat(persisted.rescoreOversample(), equalTo(NO_CALIBRATED_OVERSAMPLE));
            }
        }
    }

    public void testBackgroundMergeUsesCodecDefaultOnEncodingDisagreement() throws IOException {
        Random rnd = random();
        int vectorsPerSegment = IvfAutoCalibration.MIN_VECTORS_FOR_CALIBRATION / 2 + 100;
        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildBackgroundMergedWithDisagreeingFlushCalibration(
                    dir,
                    rnd,
                    8,
                    vectorsPerSegment,
                    VPC
                )
            ) {
                assertEquals(1, reader.leaves().size());
                IvfSegmentConfig persisted = ESNextRescoreOversampleTestFixture.readPersistedSegmentConfig(
                    reader.leaves().getFirst().reader()
                );
                assertNotNull(persisted);
                assertThat(persisted.quantEncoding(), is(ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY));
                assertThat(persisted.rescoreOversample(), equalTo(NO_CALIBRATED_OVERSAMPLE));
            }
        }
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

    private MergeState mergeStateWithVectorCount(Directory dir, FieldInfo fieldInfo, int vectorCount) throws IOException {
        KnnVectorsReader reader = heapVectorReader(fieldInfo, randomHeapVectors(vectorCount, DIM));
        return mergeState(new KnnVectorsReader[] { reader }, new Bits[] { liveDocs(vectorCount) }, backgroundSegmentInfo(dir), fieldInfo);
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

    private static MergeState mergeState(KnnVectorsReader[] readers, Bits[] liveDocsBits, SegmentInfo segmentInfo, FieldInfo fieldInfo)
        throws IOException {
        FieldInfos[] fieldInfos = null;
        if (fieldInfo != null && readers != null) {
            fieldInfos = new FieldInfos[readers.length];
            for (int i = 0; i < readers.length; i++) {
                FloatVectorValues vectors = readers[i].getFloatVectorValues(fieldInfo.name);
                fieldInfos[i] = vectors != null ? new FieldInfos(new FieldInfo[] { fieldInfo }) : new FieldInfos(new FieldInfo[0]);
            }
        }
        return mergeState(readers, liveDocsBits, segmentInfo, fieldInfos);
    }

    private static MergeState mergeState(
        KnnVectorsReader[] readers,
        Bits[] liveDocsBits,
        SegmentInfo segmentInfo,
        FieldInfos[] fieldInfos
    ) {
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

    private static FloatVectorValues randomHeapVectors(int count, int dim) throws IOException {
        Random rnd = random();
        List<float[]> vecs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            float[] v = new float[dim];
            for (int d = 0; d < dim; d++) {
                v[d] = rnd.nextFloat();
            }
            org.apache.lucene.util.VectorUtil.l2normalize(v);
            vecs.add(v);
        }
        return KMeansFloatVectorValues.build(vecs, null, dim);
    }

}
