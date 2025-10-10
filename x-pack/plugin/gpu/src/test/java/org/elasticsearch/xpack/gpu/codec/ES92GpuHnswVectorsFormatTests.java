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
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.xpack.gpu.GPUSupport;
import org.junit.BeforeClass;

// CuVS prints tons of logs to stdout
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "https://github.com/rapidsai/cuvs/issues/1310")
public class ES92GpuHnswVectorsFormatTests extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    static Codec codec;

    /** Format that mostly builds indices on the GPU rarely using the CPU, because of the tinySegmentThreshold. */
    static class ES92GpuHnswVectorsFormatTinyOne extends ES92GpuHnswVectorsFormat {
        ES92GpuHnswVectorsFormatTinyOne() {
            super(CuVSResourceManager::pooling, ES92GpuHnswVectorsFormat.DEFAULT_MAX_CONN, ES92GpuHnswVectorsFormat.DEFAULT_BEAM_WIDTH, 1);
        }
    }

    @BeforeClass
    public static void beforeClass() {
        assumeTrue("cuvs not supported", GPUSupport.isSupported(false));
        codec = TestUtil.alwaysKnnVectorsFormat(new ES92GpuHnswVectorsFormatTinyOne());
    }

    @Override
    protected Codec getCodec() {
        return codec;
    }

    public void testKnnVectorsFormat() {
        KnnVectorsFormat knnVectorsFormat = new ES92GpuHnswVectorsFormatTinyOne();
        String expectedStr = "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, "
            + "maxConn=16, beamWidth=128, tinySegmentsThreshold=1, flatVectorFormat=Lucene99FlatVectorsFormat)";
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
    public void testRandomBytes() throws Exception {
        // No bytes support
    }

    @Override
    public void testSortedIndexBytes() throws Exception {
        // No bytes support
    }

    @Override
    public void testByteVectorScorerIteration() throws Exception {
        // No bytes support
    }

    @Override
    public void testEmptyByteVectorData() throws Exception {
        // No bytes support
    }

    @Override
    public void testMergingWithDifferentByteKnnFields() throws Exception {
        // No bytes support
    }

    @Override
    public void testMismatchedFields() throws Exception {
        // No bytes support
    }

}
