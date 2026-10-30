/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.gpu.CuVSGPUSupport;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.codec.vectors.ESBaseKnnVectorsFormatTestCase;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.junit.BeforeClass;

import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.elasticsearch.test.ESTestCase.randomFrom;

/**
 * Runs the standard KNN format test suite against {@link ES97GpuHnswBBQVectorsFormat}.
 * Requires a GPU — skipped on non-GPU nodes.
 */
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "https://github.com/rapidsai/cuvs/issues/1310")
public class ES97GpuHnswBBQVectorsFormatTests extends ESBaseKnnVectorsFormatTestCase {

    static Codec codec;

    @BeforeClass
    public static void beforeClass() {
        var gpuSupport = CuVSGPUSupport.instance();
        assumeTrue("cuvs not supported", gpuSupport.isSupported());
        codec = TestUtil.alwaysKnnVectorsFormat(
            new ES97GpuHnswBBQVectorsFormat(gpuSupport.getTotalGpuMemory(), DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH, false, 0, false)
        );
    }

    @Override
    protected Codec getCodec() {
        return codec;
    }

    @Override
    protected VectorSimilarityFunction randomSimilarity() {
        // bbq_hnsw builds the GPU graph from raw floats, so all similarities are supported
        var randomGPUSupportedSimilarity = randomFrom(DenseVectorFieldMapper.VectorSimilarity.values());
        return randomGPUSupportedSimilarity.vectorSimilarityFunction(IndexVersion.current(), DenseVectorFieldMapper.ElementType.FLOAT);
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

    @Override
    public void testWriterByteVectorRamEstimate() {
        // No bytes support
    }

    @Override
    protected boolean supportsFloatVectorFallback() {
        return false;
    }
}
