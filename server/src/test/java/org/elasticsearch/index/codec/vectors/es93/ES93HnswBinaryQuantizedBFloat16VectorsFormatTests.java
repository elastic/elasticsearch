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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.closeTo;

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
        // estimate is different due to bfloat16
    }

    @Override
    public void testSingleVectorCase() throws Exception {
        AssertionError err = expectThrows(AssertionError.class, super::testSingleVectorCase);
        assertFloatsWithinBounds(err);
    }

    @Override
    public void testRandom() throws Exception {
        AssertionError err = expectThrows(AssertionError.class, super::testRandom);
        assertFloatsWithinBounds(err);
    }

    @Override
    public void testRandomWithUpdatesAndGraph() throws Exception {
        AssertionError err = expectThrows(AssertionError.class, super::testRandomWithUpdatesAndGraph);
        assertFloatsWithinBounds(err);
    }

    @Override
    public void testSparseVectors() throws Exception {
        AssertionError err = expectThrows(AssertionError.class, super::testSparseVectors);
        assertFloatsWithinBounds(err);
    }

    @Override
    public void testVectorValuesReportCorrectDocs() throws Exception {
        AssertionError err = expectThrows(AssertionError.class, super::testVectorValuesReportCorrectDocs);
        assertFloatsWithinBounds(err);
    }

    private static final Pattern FLOAT_ASSERTION_FAILURE = Pattern.compile(".*expected:<([0-9.-]+)> but was:<([0-9.-]+)>");

    private static void assertFloatsWithinBounds(AssertionError error) {
        Matcher m = FLOAT_ASSERTION_FAILURE.matcher(error.getMessage());
        if (m.matches() == false) {
            throw error;    // nothing to do with us, just rethrow
        }

        // numbers just need to be in the same vicinity
        double expected = Double.parseDouble(m.group(1));
        double actual = Double.parseDouble(m.group(2));
        double allowedError = expected * 0.01;  // within 1%
        assertThat(error.getMessage(), actual, closeTo(expected, allowedError));
    }
}
