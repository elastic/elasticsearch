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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextRescoreOversampleTestFixture;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextRescoreOversampleTestFixture.CALIBRATION_CANDIDATE_ENCODINGS;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextRescoreOversampleTestFixture.CALIBRATION_RERANK_OVERSAMPLES;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link IvfAutoCalibration} merge decision logic.
 */
public class IvfAutoCalibrationTests extends ESTestCase {

    private static final int DIM = 4;
    private static final int VPC = 128;

    private static final IvfSegmentConfig CODEC_DEFAULT = IvfSegmentConfig.fromCodecDefaults(
        ESNextDiskBBQVectorsFormat.CentroidIndexFormat.FLAT,
        ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY,
        false
    );

    public void testSelectBelowMinVectorsReturnsDefaultOversample() throws IOException {
        IvfAutoCalibration selector = new IvfAutoCalibration(VPC);
        FieldInfo fieldInfo = vectorFieldInfo("f");
        try (Directory dir = newDirectory()) {
            MergeState mergeState = mergeStateWithVectorCount(dir, fieldInfo, 500);

            IvfSegmentConfig config = selector.resolve(fieldInfo, mergeState, CODEC_DEFAULT);

            assertThat(config.quantEncoding(), is(CODEC_DEFAULT.quantEncoding()));
            assertThat(config.usePrecondition(), is(CODEC_DEFAULT.usePrecondition()));
            assertThat(config.rescoreOversample(), equalTo(CODEC_DEFAULT.rescoreOversample()));
        }
    }

    public void testIsBoundedForceMerge() throws IOException {
        try (Directory dir = newDirectory()) {
            MergeState forceMerge = mergeState(dir, new KnnVectorsReader[0], new Bits[0], forceMergeSegmentInfo(dir));
            assertTrue(IvfAutoCalibration.isBoundedForceMerge(forceMerge));
        }

        try (Directory bgDir = newDirectory()) {
            MergeState background = mergeState(bgDir, new KnnVectorsReader[0], new Bits[0], backgroundSegmentInfo(bgDir));
            assertFalse(IvfAutoCalibration.isBoundedForceMerge(background));
        }

        try (Directory numDir = newDirectory()) {
            SegmentInfo nonIntInfo = backgroundSegmentInfo(numDir);
            nonIntInfo.addDiagnostics(Map.of("mergeMaxNumSegments", "notAnInt"));
            MergeState nonInt = mergeState(numDir, new KnnVectorsReader[0], new Bits[0], nonIntInfo);
            assertFalse(IvfAutoCalibration.isBoundedForceMerge(nonInt));
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

            IvfAutoCalibration selector = new IvfAutoCalibration(VPC);
            IvfSegmentConfig reused = selector.selectFromMergeState(fieldInfo, mergeState);

            assertThat(reused, notNullValue());
            assertThat(reused.quantEncoding(), is(ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY));
            assertThat(reused.rescoreOversample(), equalTo(3.2f));
            assertFalse(reused.usePrecondition());
        }
    }

    /**
     * Calibration must handle a merge that mixes segments with no deletes ({@code liveDocs == null}) and segments
     * with deleted docs (non-null {@code liveDocs}). Metadata reuse weights each agreeing segment by its <em>live</em>
     * doc count, so deleted docs (which won't survive the merge) don't skew the reused oversample.
     */
    public void testSelectFromMergeStateReusesAcrossSegmentsWithAndWithoutDeletes() throws IOException {
        FieldInfo fieldInfo = vectorFieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
        // Both segments agree on encoding, so metadata is reused rather than re-calibrated.
        StubCalibrationKnnVectorsReader noDeletes = new StubCalibrationKnnVectorsReader(
            ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY,
            2f,
            false
        );
        StubCalibrationKnnVectorsReader withDeletes = new StubCalibrationKnnVectorsReader(
            ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY,
            4f,
            false
        );
        int maxDocA = 8000; // no deletes -> 8000 live
        int maxDocB = 4000; // 2000 deleted -> 2000 live
        try (Directory dir = newDirectory()) {
            // segment A has no deletes (liveDocs == null); segment B has 2000 of 4000 docs deleted.
            MergeState mergeState = mergeStateWithMaxDocs(
                new KnnVectorsReader[] { noDeletes, withDeletes },
                new Bits[] { null, liveDocsWithDeletes(maxDocB, 2000) },
                new int[] { maxDocA, maxDocB },
                backgroundSegmentInfo(dir),
                fieldInfo
            );

            IvfAutoCalibration selector = new IvfAutoCalibration(VPC);
            IvfSegmentConfig reused = selector.selectFromMergeState(fieldInfo, mergeState);

            assertThat(reused, notNullValue());
            assertThat(reused.quantEncoding(), is(ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY));
            // Weighted by LIVE docs per segment (deletes excluded): (2*8000 + 4*2000) / (8000 + 2000) = 2.4
            // Weighting by maxDoc would instead give (2*8000 + 4*4000) / (8000 + 4000) = 2.667.
            assertThat(reused.rescoreOversample(), equalTo(2.4f));
            assertFalse(reused.usePrecondition());
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

            IvfAutoCalibration selector = new IvfAutoCalibration(VPC);
            assertThat(selector.selectFromMergeState(fieldInfo, mergeState), nullValue());
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

                IvfAutoCalibration selector = new IvfAutoCalibration(VPC);
                IvfSegmentConfig reused = selector.selectFromMergeState(fieldInfo, mergeState);

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

    public void testCalibrateOnHeapVectors() throws IOException {
        FloatVectorValues vectors = randomHeapVectors(between(500, 1500), DIM);
        IvfAutoCalibration selector = new IvfAutoCalibration(VPC);

        IvfSegmentConfig config = selector.calibrate(vectors, VectorSimilarityFunction.EUCLIDEAN);

        assertThat(config.quantEncoding(), notNullValue());
        assertTrue(CALIBRATION_CANDIDATE_ENCODINGS.contains(config.quantEncoding()));
        assertTrue(Float.isFinite(config.rescoreOversample()));
        assertTrue(config.rescoreOversample() > 0f);
    }

    public void testSelectFromMergeStateReturnsNullWhenNoCalibratedSegments() throws IOException {
        FieldInfo fieldInfo = vectorFieldInfo(ESNextRescoreOversampleTestFixture.FIELD_NAME);
        try (Directory dir = newDirectory()) {
            MergeState mergeState = mergeState(dir, new KnnVectorsReader[0], new Bits[0], backgroundSegmentInfo(dir));

            IvfAutoCalibration selector = new IvfAutoCalibration(VPC);
            assertThat(selector.selectFromMergeState(fieldInfo, mergeState), nullValue());
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

            IvfAutoCalibration selector = new IvfAutoCalibration(VPC);
            IvfSegmentConfig reused = selector.selectFromMergeState(fieldInfo, mergeState);

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

            IvfAutoCalibration selector = new IvfAutoCalibration(VPC);
            assertThat(selector.selectFromMergeState(fieldInfo, mergeState), nullValue());
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

            IvfAutoCalibration selector = new IvfAutoCalibration(VPC);
            IvfSegmentConfig reused = selector.selectFromMergeState(fieldInfo, mergeState);

            assertThat(reused, notNullValue());
            assertTrue(reused.usePrecondition());
            assertThat(reused.rescoreOversample(), equalTo(2.8f));
        }
    }

    public void testSelectBoundedForceMergeRunsCalibrate() throws IOException {
        TrackingSelector selector = new TrackingSelector(VPC);
        FieldInfo fieldInfo = vectorFieldInfo("f");
        try (Directory dir = newDirectory()) {
            FloatVectorValues vectors = AutoCalibrationVectorFixtures.clusteredHeapVectors(
                IvfAutoCalibration.MIN_VECTORS_FOR_CALIBRATION,
                DIM,
                16,
                42L
            );
            MergeState mergeState = mergeStateWithVectors(dir, fieldInfo, vectors, forceMergeSegmentInfo(dir));

            IvfSegmentConfig config = selector.resolve(fieldInfo, mergeState, CODEC_DEFAULT);

            assertThat(config.quantEncoding(), notNullValue());
            assertThat(selector.calibrateInvocations, equalTo(1));
            assertTrue(Float.isFinite(config.rescoreOversample()));
        }
    }

    public void testSelectBoundedForceMergeUsesStubCalibrateResult() throws IOException {
        TrackingSelector selector = new TrackingSelector(VPC) {
            @Override
            protected IvfSegmentConfig calibrate(
                FloatVectorValues floatVectorValues,
                VectorSimilarityFunction similarityFunction,
                int realNumVectors,
                IvfAutoCalibration.CalibrationMode mode
            ) {
                return new IvfSegmentConfig(
                    ESNextDiskBBQVectorsFormat.CentroidIndexFormat.FLAT,
                    ESNextDiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY,
                    false,
                    2.5f
                );
            }
        };
        FieldInfo fieldInfo = vectorFieldInfo("f");
        try (Directory dir = newDirectory()) {
            FloatVectorValues vectors = AutoCalibrationVectorFixtures.clusteredHeapVectors(
                IvfAutoCalibration.MIN_VECTORS_FOR_CALIBRATION,
                DIM,
                16,
                42L
            );
            MergeState mergeState = mergeStateWithVectors(dir, fieldInfo, vectors, forceMergeSegmentInfo(dir));

            IvfSegmentConfig config = selector.resolve(fieldInfo, mergeState, CODEC_DEFAULT);

            assertThat(config.quantEncoding(), is(ESNextDiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY));
            assertThat(config.rescoreOversample(), equalTo(2.5f));
            assertEquals(0, selector.calibrateInvocations);
        }
    }

    public void testSelectIOExceptionOnBoundedForceMergeFallsBack() throws IOException {
        IvfAutoCalibration selector = new IvfAutoCalibration(VPC);
        FieldInfo fieldInfo = vectorFieldInfo("f");
        try (Directory dir = newDirectory()) {
            FloatVectorValues vectors = AutoCalibrationVectorFixtures.failingHeapVectors(
                IvfAutoCalibration.MIN_VECTORS_FOR_CALIBRATION,
                DIM,
                44L
            );
            MergeState mergeState = mergeStateWithVectors(dir, fieldInfo, vectors, forceMergeSegmentInfo(dir));

            IvfSegmentConfig config = selector.resolve(fieldInfo, mergeState, CODEC_DEFAULT);

            assertThat(config.quantEncoding(), is(CODEC_DEFAULT.quantEncoding()));
            assertFalse(config.usePrecondition());
            assertThat(config.rescoreOversample(), equalTo(CODEC_DEFAULT.rescoreOversample()));
        }
    }

    public void testSelectIOExceptionOnBackgroundMergeFallsBack() throws IOException {
        IvfAutoCalibration selector = new IvfAutoCalibration(VPC);
        FieldInfo fieldInfo = vectorFieldInfo("f");
        try (Directory dir = newDirectory()) {
            FloatVectorValues vectors = AutoCalibrationVectorFixtures.failingHeapVectors(
                IvfAutoCalibration.MIN_VECTORS_FOR_CALIBRATION,
                DIM,
                45L
            );
            MergeState mergeState = mergeStateWithVectors(dir, fieldInfo, vectors, backgroundSegmentInfo(dir));

            IvfSegmentConfig config = selector.resolve(fieldInfo, mergeState, CODEC_DEFAULT);

            assertThat(config.quantEncoding(), is(CODEC_DEFAULT.quantEncoding()));
            assertThat(config.rescoreOversample(), equalTo(CODEC_DEFAULT.rescoreOversample()));
        }
    }

    public void testCalibrateOnLargeSyntheticCorpus() throws IOException {
        FloatVectorValues vectors = AutoCalibrationVectorFixtures.clusteredHeapVectors(10_500, 8, 32, 46L);
        IvfAutoCalibration selector = new IvfAutoCalibration(VPC);

        IvfSegmentConfig config = selector.calibrate(vectors, VectorSimilarityFunction.EUCLIDEAN);

        assertThat(config.quantEncoding(), notNullValue());
        assertTrue(Float.isFinite(config.rescoreOversample()));
        assertTrue(config.rescoreOversample() > 0f);
        assertTrue(CALIBRATION_CANDIDATE_ENCODINGS.contains(config.quantEncoding()));
    }

    public void testCalibrateFullOnSyntheticCorpus() throws IOException {
        FloatVectorValues vectors = AutoCalibrationVectorFixtures.clusteredHeapVectors(
            IvfAutoCalibration.MIN_VECTORS_FOR_CALIBRATION + 500,
            8,
            32,
            47L
        );
        IvfAutoCalibration selector = new IvfAutoCalibration(VPC);

        IvfSegmentConfig config = selector.calibrate(vectors, VectorSimilarityFunction.EUCLIDEAN);

        assertThat(config.quantEncoding(), notNullValue());
        assertTrue(CALIBRATION_CANDIDATE_ENCODINGS.contains(config.quantEncoding()));
        assertTrue(Float.isFinite(config.rescoreOversample()));
        assertTrue(config.rescoreOversample() > 0f);
    }

    public void testCalibrateDotProductSimilarity() throws IOException {
        assertCalibrateProducesFiniteConfig(VectorSimilarityFunction.DOT_PRODUCT);
    }

    public void testCalibrateMaximumInnerProductSimilarity() throws IOException {
        assertCalibrateProducesFiniteConfig(VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT);
    }

    public void testCalibrateCosineSimilarity() throws IOException {
        assertCalibrateProducesFiniteConfig(VectorSimilarityFunction.COSINE);
    }

    public void testProductionMergeResolverPersistsCalibratedConfig() throws IOException {
        Random rnd = random();
        int vectorsPerSegment = IvfAutoCalibration.MIN_VECTORS_FOR_CALIBRATION / 2 + 100;
        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildForceMergedWithDisagreeingFlushCalibration(
                    dir,
                    8,
                    vectorsPerSegment,
                    VPC
                )
            ) {
                IvfSegmentConfig persisted = ESNextRescoreOversampleTestFixture.readPersistedSegmentConfig(
                    reader.leaves().getFirst().reader()
                );
                assertNotNull(persisted);
                assertTrue(CALIBRATION_CANDIDATE_ENCODINGS.contains(persisted.quantEncoding()));
                assertTrue(Float.isFinite(persisted.rescoreOversample()));
                assertTrue(
                    "calibrated oversample should be a rerank ratio, not flush-injected 2f",
                    CALIBRATION_RERANK_OVERSAMPLES.contains(persisted.rescoreOversample()) || persisted.rescoreOversample() != 2f
                );
            }
        }
    }

    public void testBackgroundMergeWithEncodingDisagreementCompletesSuccessfully() throws IOException, InterruptedException {
        Random rnd = random();
        int vectorsPerSegment = IvfAutoCalibration.MIN_VECTORS_FOR_CALIBRATION / 2 + 100;
        IvfAutoCalibration calibration = new IvfAutoCalibration(VPC);
        try (Directory dir = newDirectory()) {
            try (
                DirectoryReader reader = ESNextRescoreOversampleTestFixture.buildBackgroundMergedWithDisagreeingFlushCalibration(
                    dir,
                    rnd,
                    8,
                    vectorsPerSegment,
                    calibration
                )
            ) {
                assertEquals(1, reader.leaves().size());
                IvfSegmentConfig persisted = ESNextRescoreOversampleTestFixture.readPersistedSegmentConfig(
                    reader.leaves().getFirst().reader()
                );
                assertNotNull(persisted);
                // The persisted encoding must be one of the calibration candidate encodings.
                assertTrue(CALIBRATION_CANDIDATE_ENCODINGS.contains(persisted.quantEncoding()));
                // Must not inherit flush-injected oversample (2f from the first flush segment).
                assertThat(persisted.rescoreOversample(), not(equalTo(2f)));
            }
        }
    }

    private void assertCalibrateProducesFiniteConfig(VectorSimilarityFunction similarityFunction) throws IOException {
        FloatVectorValues vectors = AutoCalibrationVectorFixtures.clusteredHeapVectors(1200, DIM, 8, similarityFunction.ordinal());
        IvfAutoCalibration selector = new IvfAutoCalibration(VPC);

        IvfSegmentConfig config = selector.calibrate(vectors, similarityFunction);

        assertThat(config.quantEncoding(), notNullValue());
        assertTrue(CALIBRATION_CANDIDATE_ENCODINGS.contains(config.quantEncoding()));
        assertTrue(Float.isFinite(config.rescoreOversample()));
        assertTrue(config.rescoreOversample() > 0f);
    }

    private MergeState mergeStateWithVectorCount(Directory dir, FieldInfo fieldInfo, int vectorCount) throws IOException {
        FloatVectorValues vectors = randomHeapVectors(vectorCount, DIM);
        return mergeStateWithVectors(dir, fieldInfo, vectors, backgroundSegmentInfo(dir));
    }

    private static MergeState mergeStateWithVectors(Directory dir, FieldInfo fieldInfo, FloatVectorValues vectors, SegmentInfo segmentInfo)
        throws IOException {
        return mergeState(
            dir,
            new KnnVectorsReader[] { heapVectorReader(fieldInfo, vectors) },
            new Bits[] { liveDocs(vectors.size()) },
            segmentInfo,
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

    private static MergeState mergeState(Directory dir, KnnVectorsReader[] readers, Bits[] liveDocsBits, SegmentInfo segmentInfo) {
        return mergeState(dir, readers, liveDocsBits, segmentInfo, (FieldInfos[]) null);
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
                FloatVectorValues segmentVectors = readers[i].getFloatVectorValues(fieldInfo.name);
                fieldInfos[i] = segmentVectors != null ? new FieldInfos(new FieldInfo[] { fieldInfo }) : new FieldInfos(new FieldInfo[0]);
            }
        }
        return mergeState(dir, readers, liveDocsBits, segmentInfo, fieldInfos);
    }

    private static MergeState mergeState(
        Directory dir,
        KnnVectorsReader[] readers,
        Bits[] liveDocsBits,
        SegmentInfo segmentInfo,
        FieldInfos[] fieldInfos
    ) {
        // maxDocs[i] = liveDocsBits[i].length() — the bit-set size equals maxDoc per segment.
        int[] maxDocs = liveDocsBits != null ? Arrays.stream(liveDocsBits).mapToInt(Bits::length).toArray() : null;
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
            maxDocs,
            null,
            null,
            false,
            null
        );
    }

    private static SegmentInfo forceMergeSegmentInfo(Directory dir) throws IOException {
        SegmentInfo info = backgroundSegmentInfo(dir);
        info.addDiagnostics(Map.of("mergeMaxNumSegments", "1"));
        return info;
    }

    /**
     * Tracks whether full {@link IvfAutoCalibration#calibrate} ran during {@link IvfAutoCalibration#resolve}.
     */
    private static class TrackingSelector extends IvfAutoCalibration {
        int calibrateInvocations;

        TrackingSelector(int vectorsPerCluster) {
            super(vectorsPerCluster);
        }

        @Override
        protected IvfSegmentConfig calibrate(
            FloatVectorValues floatVectorValues,
            VectorSimilarityFunction similarityFunction,
            int realNumVectors,
            IvfAutoCalibration.CalibrationMode mode
        ) throws IOException {
            calibrateInvocations++;
            return super.calibrate(floatVectorValues, similarityFunction, realNumVectors, mode);
        }
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

    /**
     * A live-docs bitset for a segment with deletes: {@code maxDoc} bits, of which the first {@code numDeleted}
     * are cleared. Like a real merge's {@code liveDocs}, {@link Bits#length()} returns {@code maxDoc}.
     */
    private static Bits liveDocsWithDeletes(int maxDoc, int numDeleted) {
        FixedBitSet bits = new FixedBitSet(maxDoc);
        bits.set(0, maxDoc);
        for (int i = 0; i < numDeleted; i++) {
            bits.clear(i);
        }
        return bits;
    }

    /**
     * Builds a {@link MergeState} with explicit per-segment {@code maxDocs} and a {@code liveDocs} array that may
     * contain {@code null} entries (segments without deletes), mirroring what Lucene passes at merge time.
     */
    private static MergeState mergeStateWithMaxDocs(
        KnnVectorsReader[] readers,
        Bits[] liveDocsBits,
        int[] maxDocs,
        SegmentInfo segmentInfo,
        FieldInfo fieldInfo
    ) {
        FieldInfos[] fieldInfos = new FieldInfos[readers.length];
        for (int i = 0; i < readers.length; i++) {
            fieldInfos[i] = new FieldInfos(new FieldInfo[] { fieldInfo });
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
            maxDocs,
            null,
            null,
            false,
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
