/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContext;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContextResolver;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

public class RunTableGateTests extends ESTestCase {

    private static final FieldContextResolver DIMENSION = (name, blockSize) -> new FieldContext(blockSize, name, null, null, true);
    private static final FieldContextResolver NON_DIMENSION = (name, blockSize) -> new FieldContext(blockSize, name, null, null, false);

    private static FieldInfo fieldInfo(String name, int number) {
        return new FieldInfo(
            name,
            number,
            false,
            false,
            false,
            IndexOptions.NONE,
            DocValuesType.SORTED,
            DocValuesSkipIndexType.NONE,
            -1,
            Collections.emptyMap(),
            0,
            0,
            0,
            0,
            VectorEncoding.BYTE,
            VectorSimilarityFunction.COSINE,
            false,
            false
        );
    }

    public void testAllowPrimarySortFieldReturnsFalse() {
        final int primarySort = randomBoolean() ? 0 : 1;
        final int maxDoc = randomIntBetween(2, 4096);
        assertFalse(
            new RunTableGate(randomBoolean() ? DIMENSION : NON_DIMENSION, primarySort, maxDoc, 1).allow(
                fieldInfo("host", primarySort),
                randomIntBetween(1, maxDoc)
            )
        );
    }

    public void testAllowNullResolverReturnsFalse() {
        final int primarySort = randomBoolean() ? 0 : 1;
        final int fieldNumber = 1 - primarySort;
        final int maxDoc = randomIntBetween(2, 4096);
        assertFalse(new RunTableGate(null, primarySort, maxDoc, 1).allow(fieldInfo("host", fieldNumber), randomIntBetween(1, maxDoc / 2)));
    }

    public void testAllowNonDimensionReturnsFalse() {
        final int primarySort = randomBoolean() ? 0 : 1;
        final int fieldNumber = 1 - primarySort;
        final int maxDoc = randomIntBetween(2, 4096);
        assertFalse(
            new RunTableGate(NON_DIMENSION, primarySort, maxDoc, 1).allow(fieldInfo("cpu", fieldNumber), randomIntBetween(1, maxDoc / 2))
        );
    }

    public void testAllowRoutingHashAdmittedDespiteNonDimension() {
        final int maxDoc = randomIntBetween(4, 4096);
        final int maxOrd = randomIntBetween(1, maxDoc / 2);
        assertTrue(new RunTableGate(NON_DIMENSION, -1, maxDoc, 1).allow(fieldInfo(TimeSeriesRoutingHashFieldMapper.NAME, 0), maxOrd));
    }

    public void testAllowRoutingHashRejectedAboveThreshold() {
        final int maxDoc = randomIntBetween(2, 4096);
        final int maxOrd = randomIntBetween(maxDoc / 2 + 1, maxDoc);
        assertFalse(new RunTableGate(NON_DIMENSION, -1, maxDoc, 1).allow(fieldInfo(TimeSeriesRoutingHashFieldMapper.NAME, 0), maxOrd));
    }

    public void testAllowMaxOrdExceedsThresholdReturnsFalse() {
        final int maxDoc = randomIntBetween(2, 4096);
        final int maxOrd = randomIntBetween(maxDoc / 2 + 1, maxDoc);
        assertFalse(new RunTableGate(randomBoolean() ? DIMENSION : NON_DIMENSION, -1, maxDoc, 1).allow(fieldInfo("host", 0), maxOrd));
    }

    public void testAllowAtThresholdBoundaryReturnsTrue() {
        final int maxDoc = randomIntBetween(2, 4096);
        assertTrue(new RunTableGate(DIMENSION, -1, maxDoc, 1).allow(fieldInfo("host", 0), maxDoc / 2));
    }

    public void testAllowDimensionBelowThresholdReturnsTrue() {
        final int maxDoc = randomIntBetween(4, 4096);
        final int maxOrd = randomIntBetween(1, maxDoc / 2);
        assertTrue(new RunTableGate(DIMENSION, -1, maxDoc, 1).allow(fieldInfo("host", 0), maxOrd));
    }

    public void testAllowAbsoluteThresholdExceededReturnsFalse() {
        final int maxDoc = randomIntBetween(2, 4096);
        final int numRuns = randomIntBetween(maxDoc / 2 + 1, maxDoc);
        assertFalse(new RunTableGate(null, -1, maxDoc, 1).allow(numRuns, randomIntBetween(1, maxDoc)));
    }

    public void testAllowAtAbsoluteBoundaryReturnsTrue() {
        final int maxDoc = randomIntBetween(16, 4096);
        final int processedDocs = randomIntBetween(1, (maxDoc - 1) / 8);
        assertTrue(new RunTableGate(null, -1, maxDoc, 1).allow(maxDoc / 2, processedDocs));
    }

    public void testAllowBeforeWarmupReturnsTrue() {
        final int maxDoc = randomIntBetween(2, 4096);
        final int processedDocs = randomIntBetween(0, (maxDoc - 1) / 8);
        final int numRuns = randomIntBetween(0, maxDoc / 2);
        assertTrue(new RunTableGate(null, -1, maxDoc, 1).allow(numRuns, processedDocs));
    }

    public void testAllowAfterWarmupWithHighRunRateReturnsFalse() {
        final int maxDoc = randomIntBetween(16, 4096);
        final int processedDocs = randomIntBetween((maxDoc + 7) / 8, maxDoc - 1);
        final int numRuns = randomIntBetween(processedDocs / 2 + 1, maxDoc / 2);
        assertFalse(new RunTableGate(null, -1, maxDoc, 1).allow(numRuns, processedDocs));
    }

    public void testAllowAfterWarmupWithLowRunRateReturnsTrue() {
        final int maxDoc = randomIntBetween(16, 4096);
        final int processedDocs = randomIntBetween((maxDoc + 7) / 8, maxDoc);
        final int numRuns = randomIntBetween(0, processedDocs / 2);
        assertTrue(new RunTableGate(null, -1, maxDoc, 1).allow(numRuns, processedDocs));
    }

    public void testAllowNormalTsdbPatternAlwaysReturnsTrue() {
        final RunTableGate gate = new RunTableGate(null, -1, 1000, 1);
        for (int doc = 0; doc < 1000; doc++) {
            final int numRuns = doc / 100 + 1;
            assertTrue("gate should not close at doc " + doc, gate.allow(numRuns, doc + 1));
        }
    }
}
