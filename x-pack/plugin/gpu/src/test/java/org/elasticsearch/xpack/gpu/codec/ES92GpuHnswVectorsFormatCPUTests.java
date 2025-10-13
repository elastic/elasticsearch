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

    static ES92GpuHnswVectorsFormat createES92GpuHnswVectorsFormat(int tinySegmentsThreshold) {
        return new ES92GpuHnswVectorsFormat(
            ES92GpuHnswVectorsFormat.DEFAULT_MAX_CONN,
            ES92GpuHnswVectorsFormat.DEFAULT_BEAM_WIDTH,
            ThrowingCuVSResourceManager.supplier,
            tinySegmentsThreshold
        );
    }

    @BeforeClass
    public static void beforeClass() {
        // Create the format that builds indices on the CPU, because of the tinySegmentThreshold
        codec = TestUtil.alwaysKnnVectorsFormat(createES92GpuHnswVectorsFormat(Integer.MAX_VALUE));
    }

    @Override
    protected Codec getCodec() {
        return codec;
    }

    public void testKnnVectorsFormatToString() {
        KnnVectorsFormat format = createES92GpuHnswVectorsFormat(1_000_000);
        String expectedStr = "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, "
            + "maxConn=16, beamWidth=128, tinySegmentsThreshold=1000000, flatVectorFormat=Lucene99FlatVectorsFormat)";
        assertEquals(expectedStr, format.toString());

        format = createES92GpuHnswVectorsFormat(Integer.MAX_VALUE);
        expectedStr = "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, "
            + "maxConn=16, beamWidth=128, tinySegmentsThreshold=2147483647, flatVectorFormat=Lucene99FlatVectorsFormat)";
        assertEquals(expectedStr, format.toString());

        // check the detail values
        format = new ES92GpuHnswVectorsFormat();
        expectedStr = "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, "
            + "maxConn=16, beamWidth=128, tinySegmentsThreshold=10000, flatVectorFormat=Lucene99FlatVectorsFormat)";
        assertEquals(expectedStr, format.toString());

        format = new ES92GpuHnswVectorsFormat(5, 6, ThrowingCuVSResourceManager.supplier, 7);
        expectedStr = "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, "
            + "maxConn=5, beamWidth=6, tinySegmentsThreshold=7, flatVectorFormat=Lucene99FlatVectorsFormat)";
        assertEquals(expectedStr, format.toString());
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
