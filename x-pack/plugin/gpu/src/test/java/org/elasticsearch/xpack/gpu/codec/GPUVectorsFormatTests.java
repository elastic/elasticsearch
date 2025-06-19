/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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

public class GPUVectorsFormatTests extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
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
    public void testSearchWithVisitedLimit() {
        // TODO
    }

    @Override
    public void testAdvance() throws Exception {
        // TODO
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

    @Override
    public void testSortedIndexBytes() throws Exception {
        super.testSortedIndexBytes();
    }
}
