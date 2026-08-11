/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

import com.nvidia.cuvs.CagraIndexParams;
import com.nvidia.cuvs.CuVSMatrix;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.gpu.CuVSGPUSupport;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Random;

import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.DEFAULT_MAX_CONN;

/**
 * Exercises the GPU format with a resource manager that randomly fails {@code tryAcquire},
 * forcing a mix of GPU and CPU fallback paths across flush calls within the same index.
 * Requires a GPU — skipped on non-GPU nodes.
 */
public class ES92GpuHnswMixedPathTests extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.configureESLogging();
    }

    static Codec codec;

    @BeforeClass
    public static void beforeClass() {
        var gpuSupport = CuVSGPUSupport.instance();
        assumeTrue("cuvs not supported", gpuSupport.isSupported());
        codec = TestUtil.alwaysKnnVectorsFormat(
            new ES92GpuHnswVectorsFormat(
                () -> new RandomlyFailingResourceManager(CuVSResourceManager.pooling(), new Random(random().nextLong())),
                gpuSupport.getTotalGpuMemory(),
                DEFAULT_MAX_CONN,
                DEFAULT_BEAM_WIDTH
            )
        );
    }

    @Override
    protected Codec getCodec() {
        return codec;
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

    @Override
    public void testWriterByteVectorRamEstimate() throws Exception {
        // No bytes support
    }

    @Override
    protected boolean supportsFloatVectorFallback() {
        return false;
    }

    /**
     * A resource manager that randomly returns null from tryAcquire to simulate GPU contention,
     * forcing some flushes to fall back to CPU graph building.
     */
    static class RandomlyFailingResourceManager implements CuVSResourceManager {
        private final CuVSResourceManager delegate;
        private final Random random;

        RandomlyFailingResourceManager(CuVSResourceManager delegate, Random random) {
            this.delegate = delegate;
            this.random = random;
        }

        @Override
        public ManagedCuVSResources acquire(
            int numVectors,
            int dims,
            CuVSMatrix.DataType dataType,
            CagraIndexParams cagraIndexParams,
            String reason
        ) throws InterruptedException, IOException {
            return delegate.acquire(numVectors, dims, dataType, cagraIndexParams, reason);
        }

        @Override
        public ManagedCuVSResources tryAcquire(
            int numVectors,
            int dims,
            CuVSMatrix.DataType dataType,
            CagraIndexParams cagraIndexParams,
            String reason
        ) throws IOException {
            if (random.nextBoolean()) {
                return null;
            }
            return delegate.tryAcquire(numVectors, dims, dataType, cagraIndexParams, reason);
        }

        @Override
        public void finishedComputation(ManagedCuVSResources resources) {
            delegate.finishedComputation(resources);
        }

        @Override
        public void release(ManagedCuVSResources resources) {
            delegate.release(resources);
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }
    }
}
