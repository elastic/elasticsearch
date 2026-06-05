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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.elasticsearch.index.codec.vectors.diskbbq.next.IvfAutoCalibration.DEFAULT_CALIBRATED_OVERSAMPLE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link IvfAutoCalibration} merge decision logic.
 */
public class IvfAutoCalibrationTests extends ESTestCase {

    private static final int DIM = 4;
    private static final int VPC = 128;

    private static final IvfSegmentConfig CODEC_DEFAULT = IvfSegmentConfig.fromCodecDefaults(
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

    public void testMergeCalibrationContextBoundedForceMerge() throws IOException {
        Random rnd = random();
        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoLeavesThenMergedOneSegment(
                    dir,
                    rnd,
                    DIM,
                    16,
                    2f,
                    4f,
                    ESNextRescoreOversampleTestFixture.productionMergeResolver(VPC),
                    DEFAULT_CALIBRATED_OVERSAMPLE
                )
            ) {
                SegmentReader segmentReader = Lucene.tryUnwrapSegmentReader(reader.leaves().getFirst().reader());
                assertNotNull(segmentReader);
                SegmentInfo mergedSegmentInfo = segmentReader.getSegmentInfo().info;
                MergeState forceMerge = mergeState(dir, new KnnVectorsReader[0], new Bits[0], mergedSegmentInfo);
                MergeCalibrationContext ctx = MergeCalibrationContext.from(forceMerge);
                assertTrue(ctx.boundedForceMerge());
                assertNotNull(ctx.mergeMaxNumSegments());
                assertThat(ctx.mergeMaxNumSegments().intValue(), equalTo(1));
            }
        }

        try (Directory bgDir = newDirectory()) {
            MergeState background = mergeState(bgDir, new KnnVectorsReader[0], new Bits[0], backgroundSegmentInfo(bgDir));
            MergeCalibrationContext bgCtx = MergeCalibrationContext.from(background);
            assertFalse(bgCtx.boundedForceMerge());
        }
    }

    public void testSelectFromMergeStateReusesWeightedOversample() throws IOException {
        FieldInfo fieldInfo = vectorFieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
        StubCalibrationKnnVectorsReader segA = new StubCalibrationKnnVectorsReader(
            ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY,
            2f,
            false
        );
        StubCalibrationKnnVectorsReader segB = new StubCalibrationKnnVectorsReader(
            ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY,
            4f,
            false
        );
        try (Directory dir = newDirectory()) {
            MergeState mergeState = mergeState(
                dir,
                new KnnVectorsReader[] { segA, segB },
                new Bits[] { liveDocs(40), liveDocs(60) },
                backgroundSegmentInfo(dir)
            );
            MergeCalibrationContext mergeCtx = MergeCalibrationContext.from(mergeState);

            IvfAutoCalibration selector = new IvfAutoCalibration();
            IvfSegmentConfig reused = selector.selectFromMergeState(fieldInfo, mergeState, mergeCtx, 100);

            assertThat(reused, notNullValue());
            assertThat(reused.quantEncoding(), is(ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY));
            assertThat(reused.rescoreOversample(), equalTo(3.2f));
            assertFalse(reused.usePrecondition());
        }
    }

    public void testSelectFromMergeStateReturnsNullOnGrowthRatio() throws IOException {
        FieldInfo fieldInfo = vectorFieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
        StubCalibrationKnnVectorsReader seg = new StubCalibrationKnnVectorsReader(
            ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY,
            2f,
            false
        );
        try (Directory dir = newDirectory()) {
            MergeState mergeState = mergeState(
                dir,
                new KnnVectorsReader[] { seg },
                new Bits[] { liveDocs(10) },
                backgroundSegmentInfo(dir)
            );
            MergeCalibrationContext mergeCtx = MergeCalibrationContext.from(mergeState);

            IvfAutoCalibration selector = new IvfAutoCalibration();
            long mergedCount = (long) (IvfAutoCalibration.RECALIBRATE_GROWTH_RATIO * 10) + 1;
            assertThat(selector.selectFromMergeState(fieldInfo, mergeState, mergeCtx, mergedCount), nullValue());
        }
    }

    public void testSelectFromMergeStateReturnsNullOnEncodingDisagreement() throws IOException {
        FieldInfo fieldInfo = vectorFieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
        StubCalibrationKnnVectorsReader segA = new StubCalibrationKnnVectorsReader(
            ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY,
            2f,
            false
        );
        StubCalibrationKnnVectorsReader segB = new StubCalibrationKnnVectorsReader(
            ESNextDiskBBQVectorsFormat.QuantEncoding.FOUR_BIT_SYMMETRIC,
            2f,
            false
        );
        try (Directory dir = newDirectory()) {
            MergeState mergeState = mergeState(
                dir,
                new KnnVectorsReader[] { segA, segB },
                new Bits[] { liveDocs(50), liveDocs(50) },
                backgroundSegmentInfo(dir)
            );
            MergeCalibrationContext mergeCtx = MergeCalibrationContext.from(mergeState);

            IvfAutoCalibration selector = new IvfAutoCalibration();
            assertThat(selector.selectFromMergeState(fieldInfo, mergeState, mergeCtx, 100), nullValue());
        }
    }

    public void testTryMergeMetadataReuseFromRealSegments() throws IOException {
        Random rnd = random();
        float oversampleA = 2f;
        float oversampleB = 4f;
        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildTwoCommitsTwoSegments(
                    dir,
                    rnd,
                    DIM,
                    32,
                    oversampleA,
                    oversampleB,
                    IvfMergeConfigResolver.useCodecDefault()
                )
            ) {
                assertEquals(2, reader.leaves().size());
                FieldInfo fieldInfo = reader.leaves()
                    .get(0)
                    .reader()
                    .getFieldInfos()
                    .fieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
                KnnVectorsReader[] readers = new KnnVectorsReader[2];
                Bits[] liveDocs = new Bits[2];
                int totalDocs = 0;
                for (int i = 0; i < 2; i++) {
                    LeafReader leaf = reader.leaves().get(i).reader();
                    readers[i] = calibrationReader(leaf);
                    int maxDoc = leaf.maxDoc();
                    liveDocs[i] = liveDocs(maxDoc);
                    totalDocs += maxDoc;
                }

                MergeState mergeState = mergeState(dir, readers, liveDocs, backgroundSegmentInfo(dir));

                IvfAutoCalibration selector = new IvfAutoCalibration();
                MergeCalibrationContext mergeCtx = MergeCalibrationContext.from(mergeState);
                IvfSegmentConfig reused = selector.selectFromMergeState(fieldInfo, mergeState, mergeCtx, totalDocs);

                assertThat(reused, notNullValue());
                assertThat(reused.quantEncoding(), is(ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY));
                float expectedOversample = (oversampleA * reader.leaves().get(0).reader().maxDoc() + oversampleB * reader.leaves()
                    .get(1)
                    .reader()
                    .maxDoc()) / totalDocs;
                assertThat(reused.rescoreOversample(), equalTo(expectedOversample));
            }
        }
    }

    public void testSelectFromMergeStateReturnsNullWhenNoCalibratedSegments() throws IOException {
        FieldInfo fieldInfo = vectorFieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
        try (Directory dir = newDirectory()) {
            MergeState mergeState = mergeState(dir, new KnnVectorsReader[0], new Bits[0], backgroundSegmentInfo(dir));
            MergeCalibrationContext mergeCtx = MergeCalibrationContext.from(mergeState);

            IvfAutoCalibration selector = new IvfAutoCalibration();
            assertThat(selector.selectFromMergeState(fieldInfo, mergeState, mergeCtx, 100), nullValue());
        }
    }

    public void testSelectFromMergeStateReusesOnPartialEncodingAgreement() throws IOException {
        FieldInfo fieldInfo = vectorFieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
        StubCalibrationKnnVectorsReader dominant = new StubCalibrationKnnVectorsReader(
            ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY,
            2f,
            false
        );
        StubCalibrationKnnVectorsReader minority = new StubCalibrationKnnVectorsReader(
            ESNextDiskBBQVectorsFormat.QuantEncoding.FOUR_BIT_SYMMETRIC,
            2f,
            false
        );
        try (Directory dir = newDirectory()) {
            MergeState mergeState = mergeState(
                dir,
                new KnnVectorsReader[] { dominant, minority },
                new Bits[] { liveDocs(85), liveDocs(15) },
                backgroundSegmentInfo(dir)
            );
            MergeCalibrationContext mergeCtx = MergeCalibrationContext.from(mergeState);

            IvfAutoCalibration selector = new IvfAutoCalibration();
            IvfSegmentConfig reused = selector.selectFromMergeState(fieldInfo, mergeState, mergeCtx, 100);

            assertThat(reused, notNullValue());
            assertThat(reused.quantEncoding(), is(ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY));
        }
    }

    public void testSelectFromMergeStateRecalibratesOnWeakEncodingAgreement() throws IOException {
        FieldInfo fieldInfo = vectorFieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
        StubCalibrationKnnVectorsReader segA = new StubCalibrationKnnVectorsReader(
            ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY,
            2f,
            false
        );
        StubCalibrationKnnVectorsReader segB = new StubCalibrationKnnVectorsReader(
            ESNextDiskBBQVectorsFormat.QuantEncoding.FOUR_BIT_SYMMETRIC,
            2f,
            false
        );
        try (Directory dir = newDirectory()) {
            MergeState mergeState = mergeState(
                dir,
                new KnnVectorsReader[] { segA, segB },
                new Bits[] { liveDocs(70), liveDocs(30) },
                backgroundSegmentInfo(dir)
            );
            MergeCalibrationContext mergeCtx = MergeCalibrationContext.from(mergeState);

            IvfAutoCalibration selector = new IvfAutoCalibration();
            assertThat(selector.selectFromMergeState(fieldInfo, mergeState, mergeCtx, 100), nullValue());
        }
    }

    public void testSelectFromMergeStateUsesPreconditionMajorityVote() throws IOException {
        FieldInfo fieldInfo = vectorFieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
        StubCalibrationKnnVectorsReader precondTrue = new StubCalibrationKnnVectorsReader(
            ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY,
            2f,
            true
        );
        StubCalibrationKnnVectorsReader precondFalse = new StubCalibrationKnnVectorsReader(
            ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY,
            4f,
            false
        );
        try (Directory dir = newDirectory()) {
            MergeState mergeState = mergeState(
                dir,
                new KnnVectorsReader[] { precondTrue, precondFalse },
                new Bits[] { liveDocs(60), liveDocs(40) },
                backgroundSegmentInfo(dir)
            );
            MergeCalibrationContext mergeCtx = MergeCalibrationContext.from(mergeState);

            IvfAutoCalibration selector = new IvfAutoCalibration();
            IvfSegmentConfig reused = selector.selectFromMergeState(fieldInfo, mergeState, mergeCtx, 100);

            assertThat(reused, notNullValue());
            assertTrue(reused.usePrecondition());
            assertThat(reused.rescoreOversample(), equalTo(2.8f));
        }
    }

    public void testSelectBoundedForceMergeUsesCodecDefault() throws IOException {
        IvfAutoCalibration selector = new IvfAutoCalibration();
        FieldInfo fieldInfo = vectorFieldInfo("f");
        IvfSegmentConfig codecDefault = IvfSegmentConfig.fromCodecDefaults(
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
                assertThat(persisted.rescoreOversample(), equalTo(DEFAULT_CALIBRATED_OVERSAMPLE));
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
                assertThat(persisted.rescoreOversample(), equalTo(DEFAULT_CALIBRATED_OVERSAMPLE));
            }
        }
    }

    private static SegmentInfo forceMergeSegmentInfo(Directory dir) throws IOException {
        SegmentInfo info = backgroundSegmentInfo(dir);
        info.addDiagnostics(Map.of("mergeMaxNumSegments", "1"));
        return info;
    }

    private static KnnVectorsReader calibrationReader(LeafReader leaf) throws IOException {
        SegmentReader segmentReader = Lucene.tryUnwrapSegmentReader(leaf);
        assertNotNull(segmentReader);
        KnnVectorsReader kvr = segmentReader.getVectorReader();
        if (kvr instanceof org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat.FieldsReader perField) {
            return perField.getFieldReader(ESNextRescoreOversampleTestFixture.FIELD_NAME);
        }
        return kvr;
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
        return mergeState(
            dir,
            new KnnVectorsReader[] { reader },
            new Bits[] { liveDocs(vectorCount) },
            backgroundSegmentInfo(dir),
            fieldInfo
        );
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

    private static MergeState mergeState(
        Directory dir,
        KnnVectorsReader[] readers,
        Bits[] liveDocsBits,
        SegmentInfo segmentInfo,
        FieldInfo fieldInfo
    ) throws IOException {
        FieldInfos[] fieldInfos = null;
        if (fieldInfo != null && readers != null) {
            fieldInfos = new FieldInfos[readers.length];
            for (int i = 0; i < readers.length; i++) {
                FloatVectorValues vectors = readers[i].getFloatVectorValues(fieldInfo.name);
                fieldInfos[i] = vectors != null ? new FieldInfos(new FieldInfo[] { fieldInfo }) : new FieldInfos(new FieldInfo[0]);
            }
        }
        return mergeState(dir, readers, liveDocsBits, segmentInfo, fieldInfos);
    }

    private static MergeState mergeState(Directory dir, KnnVectorsReader[] readers, Bits[] liveDocsBits, SegmentInfo segmentInfo) {
        return mergeState(dir, readers, liveDocsBits, segmentInfo, (FieldInfos[]) null);
    }

    private static MergeState mergeState(
        Directory dir,
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

    /**
     * Minimal {@link KnnVectorsReader} exposing calibration metadata for merge reuse tests.
     */
    private static final class StubCalibrationKnnVectorsReader extends KnnVectorsReader implements CalibrationAwareReader {

        private final ESNextDiskBBQVectorsFormat.QuantEncoding encoding;
        private final float oversample;
        private final boolean precondition;

        StubCalibrationKnnVectorsReader(ESNextDiskBBQVectorsFormat.QuantEncoding encoding, float oversample, boolean precondition) {
            this.encoding = encoding;
            this.oversample = oversample;
            this.precondition = precondition;
        }

        @Override
        public float getOversampleFactor(FieldInfo fieldInfo) {
            return oversample;
        }

        @Override
        public boolean shouldPrecondition(FieldInfo fieldInfo) {
            return precondition;
        }

        @Override
        public ESNextDiskBBQVectorsFormat.QuantEncoding getQuantEncoding(FieldInfo fieldInfo) {
            return encoding;
        }

        @Override
        public void checkIntegrity() {}

        @Override
        public org.apache.lucene.index.FloatVectorValues getFloatVectorValues(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.apache.lucene.index.ByteVectorValues getByteVectorValues(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void search(
            String field,
            float[] target,
            org.apache.lucene.search.KnnCollector knnCollector,
            org.apache.lucene.search.AcceptDocs acceptDocs
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void search(
            String field,
            byte[] target,
            org.apache.lucene.search.KnnCollector knnCollector,
            org.apache.lucene.search.AcceptDocs acceptDocs
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
            return Map.of();
        }

        @Override
        public void close() {}
    }
}
