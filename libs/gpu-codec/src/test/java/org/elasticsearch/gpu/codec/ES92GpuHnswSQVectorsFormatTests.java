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
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.gpu.GPUSupport;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.junit.BeforeClass;

import static org.elasticsearch.test.ESTestCase.randomFrom;

@LuceneTestCase.SuppressSysoutChecks(bugUrl = "https://github.com/rapidsai/cuvs/issues/1310")
public class ES92GpuHnswSQVectorsFormatTests extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    static Codec codec;

    @BeforeClass
    public static void beforeClass() {
        assumeTrue("cuvs not supported", GPUSupport.isSupported());
        codec = TestUtil.alwaysKnnVectorsFormat(new ES92GpuHnswSQVectorsFormat());
    }

    @Override
    protected Codec getCodec() {
        return codec;
    }

    @Override
    protected VectorSimilarityFunction randomSimilarity() {
        var randomGPUSupportedSimilarity = randomFrom(
            DenseVectorFieldMapper.VectorSimilarity.L2_NORM,
            DenseVectorFieldMapper.VectorSimilarity.COSINE,
            DenseVectorFieldMapper.VectorSimilarity.DOT_PRODUCT
        );

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
}
