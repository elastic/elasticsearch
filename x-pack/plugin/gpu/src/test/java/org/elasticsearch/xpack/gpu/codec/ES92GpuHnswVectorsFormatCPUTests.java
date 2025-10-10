/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.gpu.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.junit.BeforeClass;

/** Tests the format while only exercising the CPU-based graph building. */
// @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 100)
public class ES92GpuHnswVectorsFormatCPUTests extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    static Codec codec;

    /** Format that only builds indices on the CPU never uses the GPU, because of the large tinySegmentThreshold. */
    static class ES92GpuHnswVectorsFormatTinyOneMillion extends ES92GpuHnswVectorsFormat {
        ES92GpuHnswVectorsFormatTinyOneMillion() {
            super(
                ThrowingCuVSResourceManager.supplier,
                ES92GpuHnswVectorsFormat.DEFAULT_MAX_CONN,
                ES92GpuHnswVectorsFormat.DEFAULT_BEAM_WIDTH,
                1_000_000
            );
        }
    }

    @BeforeClass
    public static void beforeClass() {
        codec = TestUtil.alwaysKnnVectorsFormat(new ES92GpuHnswVectorsFormatTinyOneMillion());
    }

    @Override
    protected Codec getCodec() {
        return codec;
    }

    public void testKnnVectorsFormatToString() {
        KnnVectorsFormat knnVectorsFormat = new ES92GpuHnswVectorsFormatTinyOneMillion();
        String expectedStr = "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, "
            + "maxConn=16, beamWidth=128, tinySegmentsThreshold=1000000, flatVectorFormat=Lucene99FlatVectorsFormat)";
        assertEquals(expectedStr, knnVectorsFormat.toString());

        // check the detail values
        knnVectorsFormat = new ES92GpuHnswVectorsFormat();
        expectedStr = "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, "
            + "maxConn=16, beamWidth=128, tinySegmentsThreshold=10000, flatVectorFormat=Lucene99FlatVectorsFormat)";
        assertEquals(expectedStr, knnVectorsFormat.toString());

        knnVectorsFormat = new ES92GpuHnswVectorsFormat(ThrowingCuVSResourceManager.supplier, 5, 6, 7);
        expectedStr = "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, "
            + "maxConn=5, beamWidth=6, tinySegmentsThreshold=7, flatVectorFormat=Lucene99FlatVectorsFormat)";
        assertEquals(expectedStr, knnVectorsFormat.toString());
    }

    @Override
    protected VectorSimilarityFunction randomSimilarity() {
        return VectorSimilarityFunction.values()[random().nextInt(VectorSimilarityFunction.values().length)];
    }

    @Override
    protected VectorEncoding randomVectorEncoding() {
        return VectorEncoding.FLOAT32;
    }

    @Override
    public void testRandomBytes() {
        // No bytes support
    }

    @Override
    public void testSortedIndexBytes() {
        // No bytes support
    }

    @Override
    public void testByteVectorScorerIteration() {
        // No bytes support
    }

    @Override
    public void testEmptyByteVectorData() {
        // No bytes support
    }

    @Override
    public void testMergingWithDifferentByteKnnFields() {
        // No bytes support
    }

    @Override
    public void testMismatchedFields() {
        // No bytes support
    }
}
