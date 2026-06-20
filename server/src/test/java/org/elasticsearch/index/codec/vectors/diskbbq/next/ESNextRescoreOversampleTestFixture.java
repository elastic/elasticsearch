/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.codec.vectors.diskbbq.CalibrationAwareReader;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfAutoCalibration;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfFlushConfigSource;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfMergeConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfSegmentConfig;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.DEFAULT_PRECONDITIONING_BLOCK_DIMENSION;
import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat.defaultFlatThreshold;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Builds lucene indexes with DiskBBQ segments carrying specific persisted
 * {@link IvfSegmentConfig#rescoreOversample()} values for search-layer and mapper tests.
 */
public final class ESNextRescoreOversampleTestFixture {

    public static final String FIELD_NAME = "f";

    private ESNextRescoreOversampleTestFixture() {}

    /** Shared codec helpers for IVF writer + merge replay. */
    public static Codec createDiskBbqCodec(IvfFlushConfigSource flushConfig, IvfMergeConfigResolver mergeResolver) {
        int vpc = 128;
        return TestUtil.alwaysKnnVectorsFormat(
            new ESNextDiskBBQVectorsFormat(
                ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY,
                vpc,
                ESNextDiskBBQVectorsFormat.MIN_CENTROIDS_PER_PARENT_CLUSTER,
                DenseVectorFieldMapper.ElementType.FLOAT,
                false,
                null,
                1,
                false,
                DEFAULT_PRECONDITIONING_BLOCK_DIMENSION,
                defaultFlatThreshold(vpc),
                null,
                flushConfig,
                mergeResolver
            )
        );
    }

    /**
     * Two commits under {@link NoMergePolicy}; first segment persists {@code oversampleSegmentA}, second {@code oversampleSegmentB}.
     */
    public static DirectoryReader buildTwoCommitsTwoSegments(
        Directory dir,
        Random rnd,
        int vectorDimensions,
        int vectorsPerSegment,
        float oversampleSegmentA,
        float oversampleSegmentB,
        IvfMergeConfigResolver mergeConfigResolver
    ) throws IOException {
        Objects.requireNonNull(dir, "dir");
        AtomicInteger flushSequence = new AtomicInteger(0);
        IvfFlushConfigSource flushConfig = (state, fieldInfo) -> {
            if (FIELD_NAME.equals(fieldInfo.name) == false) {
                return Optional.empty();
            }
            int seq = flushSequence.getAndIncrement();
            float ov = seq == 0 ? oversampleSegmentA : oversampleSegmentB;
            return Optional.of(new IvfSegmentConfig(ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY, false, ov));
        };
        Codec codec = createDiskBbqCodec(flushConfig, mergeConfigResolver);
        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer()).setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE);

        writeTwoCommits(rnd, vectorsPerSegment, vectorDimensions, dir, iwc);
        return DirectoryReader.open(dir);
    }

    /**
     * Two commits under {@link NoMergePolicy}, then force-merge to one segment. The merge-time
     * {@link IvfMergeConfigResolver} controls the persisted oversample on the output segment (flush-time values
     * are overwritten for the merged artifact).
     */
    public static DirectoryReader buildTwoLeavesThenMergedOneSegment(
        Directory dir,
        Random rnd,
        int vectorDimensions,
        int vectorsPerSegment,
        float oversampleSegmentA,
        float oversampleSegmentB,
        IvfMergeConfigResolver mergeConfigResolverForBothPhases,
        float expectedOversampleAfterMerge
    ) throws IOException {
        AtomicInteger flushSequence = new AtomicInteger(0);
        IvfFlushConfigSource flushConfig = (state, fieldInfo) -> {
            if (FIELD_NAME.equals(fieldInfo.name) == false) {
                return Optional.empty();
            }
            int seq = flushSequence.getAndIncrement();
            float ov = seq == 0 ? oversampleSegmentA : oversampleSegmentB;
            return Optional.of(new IvfSegmentConfig(ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY, false, ov));
        };
        Codec codec = createDiskBbqCodec(flushConfig, mergeConfigResolverForBothPhases);

        IndexWriterConfig iwcNoMerge = new IndexWriterConfig(new StandardAnalyzer()).setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE);
        writeTwoCommits(rnd, vectorsPerSegment, vectorDimensions, dir, iwcNoMerge);

        IndexWriterConfig iwcMerge = new IndexWriterConfig(new StandardAnalyzer()).setCodec(codec);
        try (IndexWriter mergeWriter = new IndexWriter(dir, iwcMerge)) {
            mergeWriter.forceMerge(1);
        }
        DirectoryReader reader = DirectoryReader.open(dir);
        assertEquals(1, reader.leaves().size());
        assertEquals(expectedOversampleAfterMerge, persistedOversampleOnLeaf(reader.leaves().getFirst().reader()), 0f);
        return reader;
    }

    private static void writeTwoCommits(Random rnd, int vectorsPerSegment, int vectorDimensions, Directory dir, IndexWriterConfig iwc)
        throws IOException {
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (int c = 0; c < 2; c++) {
                for (int i = 0; i < vectorsPerSegment; i++) {
                    Document d = new Document();
                    // IVF rejects COSINE similarity (see IVFVectorsWriter#addField)
                    d.add(new KnnFloatVectorField(FIELD_NAME, randomUnitVector(rnd, vectorDimensions), VectorSimilarityFunction.EUCLIDEAN));
                    writer.addDocument(d);
                }
                writer.commit();
            }
        }
    }

    /**
     * Builds two flushed segments with disagreeing calibration metadata, then force-merges to one segment
     *  so merge-time auto-calibration runs.
     */
    public static DirectoryReader buildForceMergedWithDisagreeingFlushCalibration(
        Directory dir,
        Random rnd,
        int vectorDimensions,
        int vectorsPerSegment,
        int vectorsPerCluster
    ) throws IOException {
        AtomicInteger flushSequence = new AtomicInteger(0);
        IvfFlushConfigSource flushConfig = (state, fieldInfo) -> {
            if (FIELD_NAME.equals(fieldInfo.name) == false) {
                return Optional.empty();
            }
            int seq = flushSequence.getAndIncrement();
            if (seq == 0) {
                return Optional.of(new IvfSegmentConfig(ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY, false, 2f));
            }
            return Optional.of(new IvfSegmentConfig(ESNextDiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY, false, 3f));
        };
        Codec codec = createDiskBbqCodec(flushConfig, IvfAutoCalibration.mergeConfigResolver(vectorsPerCluster));
        IndexWriterConfig iwcNoMerge = new IndexWriterConfig(new StandardAnalyzer()).setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE);
        writeTwoCommits(rnd, vectorsPerSegment, vectorDimensions, dir, iwcNoMerge);

        IndexWriterConfig iwcMerge = new IndexWriterConfig(new StandardAnalyzer()).setCodec(codec);
        try (IndexWriter mergeWriter = new IndexWriter(dir, iwcMerge)) {
            mergeWriter.forceMerge(1);
        }
        DirectoryReader reader = DirectoryReader.open(dir);
        assertEquals(1, reader.leaves().size());
        return reader;
    }

    /**
     * Two flushed segments with disagreeing calibration metadata, merged by a background tiered merge
     * (not force-merge), so {@link IvfAutoCalibration} falls back to codec default when metadata reuse fails.
     */
    public static DirectoryReader buildBackgroundMergedWithDisagreeingFlushCalibration(
        Directory dir,
        Random rnd,
        int vectorDimensions,
        int vectorsPerSegment,
        int vectorsPerCluster
    ) throws IOException {
        AtomicInteger flushSequence = new AtomicInteger(0);
        IvfFlushConfigSource flushConfig = (state, fieldInfo) -> {
            if (FIELD_NAME.equals(fieldInfo.name) == false) {
                return Optional.empty();
            }
            int seq = flushSequence.getAndIncrement();
            if (seq == 0) {
                return Optional.of(new IvfSegmentConfig(ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY, false, 2f));
            }
            return Optional.of(new IvfSegmentConfig(ESNextDiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY, false, 3f));
        };
        Codec codec = createDiskBbqCodec(flushConfig, IvfAutoCalibration.mergeConfigResolver(vectorsPerCluster));
        IndexWriterConfig iwcNoMerge = new IndexWriterConfig(new StandardAnalyzer()).setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE);
        writeTwoCommits(rnd, vectorsPerSegment, vectorDimensions, dir, iwcNoMerge);

        TieredMergePolicy mergePolicy = new TieredMergePolicy();
        mergePolicy.setSegmentsPerTier(2);
        mergePolicy.setMaxMergeAtOnce(10);
        IndexWriterConfig iwcMerge = new IndexWriterConfig(new StandardAnalyzer()).setCodec(codec).setMergePolicy(mergePolicy);
        try (IndexWriter mergeWriter = new IndexWriter(dir, iwcMerge)) {
            for (int i = 0; i < vectorsPerSegment; i++) {
                Document d = new Document();
                d.add(new KnnFloatVectorField(FIELD_NAME, randomUnitVector(rnd, vectorDimensions), VectorSimilarityFunction.EUCLIDEAN));
                mergeWriter.addDocument(d);
            }
            mergeWriter.commit();
        }
        DirectoryReader reader = DirectoryReader.open(dir);
        assertEquals("background merge should collapse disagreeing segments", 1, reader.leaves().size());
        return reader;
    }

    public static IvfSegmentConfig readPersistedSegmentConfig(LeafReader leaf) throws IOException {
        ESNextDiskBBQVectorsFormat.QuantEncoding encoding = persistedQuantEncodingOnLeaf(leaf);
        if (encoding == null) {
            return null;
        }
        return new IvfSegmentConfig(encoding, persistedPreconditionOnLeaf(leaf), persistedOversampleOnLeaf(leaf));
    }

    public static ESNextDiskBBQVectorsFormat.QuantEncoding persistedQuantEncodingOnLeaf(LeafReader leaf) throws IOException {
        CalibrationAwareReader reader = calibrationAwareReaderOnLeaf(leaf);
        if (reader == null) {
            return null;
        }
        FieldInfo fieldInfo = fieldInfoOnLeaf(leaf);
        return fieldInfo == null ? null : reader.getQuantEncoding(fieldInfo);
    }

    public static boolean persistedPreconditionOnLeaf(LeafReader leaf) throws IOException {
        CalibrationAwareReader reader = calibrationAwareReaderOnLeaf(leaf);
        if (reader == null) {
            return false;
        }
        FieldInfo fieldInfo = fieldInfoOnLeaf(leaf);
        return fieldInfo != null && reader.shouldPrecondition(fieldInfo);
    }

    public static float persistedOversampleOnLeaf(LeafReader leaf) throws IOException {
        CalibrationAwareReader reader = calibrationAwareReaderOnLeaf(leaf);
        if (reader == null) {
            return Float.NaN;
        }
        FieldInfo fieldInfo = fieldInfoOnLeaf(leaf);
        return fieldInfo == null ? Float.NaN : reader.getOversampleFactor(fieldInfo);
    }

    private static FieldInfo fieldInfoOnLeaf(LeafReader leaf) throws IOException {
        SegmentReader segmentReader = Lucene.tryUnwrapSegmentReader(leaf);
        if (segmentReader == null) {
            return null;
        }
        return segmentReader.getFieldInfos().fieldInfo(FIELD_NAME);
    }

    private static CalibrationAwareReader calibrationAwareReaderOnLeaf(LeafReader leaf) throws IOException {
        SegmentReader segmentReader = Lucene.tryUnwrapSegmentReader(leaf);
        if (segmentReader == null) {
            return null;
        }
        KnnVectorsReader kvr = segmentReader.getVectorReader();
        if (kvr instanceof PerFieldKnnVectorsFormat.FieldsReader perField) {
            kvr = perField.getFieldReader(FIELD_NAME);
        }
        if (kvr instanceof CalibrationAwareReader calibrationAwareReader) {
            return calibrationAwareReader;
        }
        return null;
    }

    public static void assertLeafOversamples(DirectoryReader reader, float oversampleSegmentA, float oversampleSegmentB)
        throws IOException {
        Set<Float> expected = Set.of(oversampleSegmentA, oversampleSegmentB);
        assertEquals(2, reader.leaves().size());
        Set<Float> found = new HashSet<>();
        for (LeafReaderContext leafCtx : reader.leaves()) {
            float v = persistedOversampleOnLeaf(leafCtx.reader());
            found.add(v);
            assertTrue("unexpected persisted oversample on leaf " + leafCtx.docBase, expected.contains(v));
        }
        assertEquals(expected, found);
    }

    private static float[] randomUnitVector(Random rnd, int dims) {
        float[] v = new float[dims];
        for (int i = 0; i < dims; i++) {
            v[i] = rnd.nextFloat();
        }
        org.apache.lucene.util.VectorUtil.l2normalize(v);
        return v;
    }
}
