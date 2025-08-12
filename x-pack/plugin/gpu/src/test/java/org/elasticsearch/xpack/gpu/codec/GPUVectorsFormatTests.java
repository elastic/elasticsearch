/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.gpu.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.xpack.gpu.GPUSupport;
import org.junit.BeforeClass;

public class GPUVectorsFormatTests extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @BeforeClass
    public static void beforeClass() {
        assumeTrue("cuvs not supported", GPUSupport.isSupported(false));
    }

    static final Codec codec = TestUtil.alwaysKnnVectorsFormat(new GPUVectorsFormat());

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
    public void testSortedIndex() throws Exception {
        // TODO: implement sorted index
    }

    @Override
    public void testFloatVectorScorerIteration() throws Exception {
        // TODO: implement sorted index
    }

    @Override
    public void testRandom() throws Exception {
        // TODO: implement sorted index
    }

    public void testToString() {
        FilterCodec customCodec = new FilterCodec("foo", Codec.getDefault()) {
            @Override
            public KnnVectorsFormat knnVectorsFormat() {
                return new GPUVectorsFormat();
            }
        };
        String expectedPattern = "GPUVectorsFormat()";
        assertEquals(expectedPattern, customCodec.knnVectorsFormat().toString());
    }

}
