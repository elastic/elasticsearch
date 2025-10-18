/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.index.VectorEncoding;

public class ES93HnswBinaryQuantizedBFloat16VectorsFormatTests extends ES93HnswBinaryQuantizedVectorsFormatTests {

    @Override
    boolean useBFloat16() {
        return true;
    }

    @Override
    protected VectorEncoding randomVectorEncoding() {
        return VectorEncoding.FLOAT32;
    }

    @Override
    public void testEmptyByteVectorData() throws Exception {
        // no bytes
    }

    @Override
    public void testMergingWithDifferentByteKnnFields() throws Exception {
        // no bytes
    }

    @Override
    public void testByteVectorScorerIteration() throws Exception {
        // no bytes
    }

    @Override
    public void testSortedIndexBytes() throws Exception {
        // no bytes
    }

    @Override
    public void testMismatchedFields() throws Exception {
        // no bytes
    }

    @Override
    public void testRandomBytes() throws Exception {
        // no bytes
    }

    @Override
    public void testWriterRamEstimate() throws Exception {
        LuceneBFloat16Tests.testWriterRamEstimate();
    }

    @Override
    public void testSingleVectorCase() throws Exception {
        testSingleVectorCase(LuceneBFloat16Tests::calculateDelta);
    }

    @Override
    public void testRandom() throws Exception {
        LuceneBFloat16Tests.testRandom(this::randomSimilarity);
    }

    @Override
    public void testRandomWithUpdatesAndGraph() throws Exception {
        LuceneBFloat16Tests.testRandomWithUpdatesAndGraph(this::assertOffHeapByteSize);
    }

    @Override
    public void testSparseVectors() throws Exception {
        LuceneBFloat16Tests.testSparseVectors(this::randomSimilarity, this::randomVectorEncoding);
    }

    @Override
    public void testVectorValuesReportCorrectDocs() throws Exception {
        LuceneBFloat16Tests.testVectorValuesReportCorrectDocs(
            this::randomSimilarity,
            this::randomVectorEncoding,
            this::assertOffHeapByteSize
        );
    }
}
