/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.gpu.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldTypeTests;
import org.elasticsearch.xpack.gpu.GPUSupport;
import org.junit.BeforeClass;

@LuceneTestCase.SuppressSysoutChecks(bugUrl = "https://github.com/rapidsai/cuvs/issues/1310")
public class ES92GpuHnswSQVectorsFormatTests extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    static Codec codec;

    @BeforeClass
    public static void beforeClass() {
        assumeTrue("cuvs not supported", GPUSupport.isSupported(false));
        codec = TestUtil.alwaysKnnVectorsFormat(new ES92GpuHnswSQVectorsFormat());
    }

    @Override
    protected Codec getCodec() {
        return codec;
    }

    @Override
    protected VectorSimilarityFunction randomSimilarity() {
        return DenseVectorFieldTypeTests.randomGPUSupportedSimilarity(DenseVectorFieldMapper.VectorIndexType.INT8_HNSW)
            .vectorSimilarityFunction(IndexVersion.current(), DenseVectorFieldMapper.ElementType.FLOAT);
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
