/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.util.bkd.BKDConfig;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.index.codec.Elasticsearch900AdaptivePointsWriter.ESTIMATED_OVERHEAD_PER_LEAF_EXCLUDING_SPLIT_VALUES;
import static org.elasticsearch.index.codec.Elasticsearch900AdaptivePointsWriter.LEAF_SIZE_ALIGNMENT;
import static org.elasticsearch.index.codec.Elasticsearch900AdaptivePointsWriter.MAX_POINTS_IN_LEAF_NODE_UPPER_BOUND;
import static org.elasticsearch.index.codec.Elasticsearch900AdaptivePointsWriter.TARGET_MAX_BKD_HEAP_BYTES;
import static org.elasticsearch.index.codec.Elasticsearch900AdaptivePointsWriter.adjustMaxPointsInLeafNode;

public class Elasticsearch900AdaptivePointsWriterTests extends ESTestCase {

    public void testResultIsAlwaysMultipleOfAlignment() {
        int bytesPerDim = randomIntBetween(1, 16);
        int maxPointsInLeafNode = randomIntBetween(128, 1024);
        long pointCount = randomLongBetween(1, 10_000_000_000L);
        int result = adjustMaxPointsInLeafNode(maxPointsInLeafNode, bytesPerDim, pointCount);
        assertEquals(0, result % LEAF_SIZE_ALIGNMENT);
    }

    public void testSmallFieldUsesDefaultRoundedUp() {
        int defaultMax = BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE;
        int bytesPerDim = 8;
        int result = adjustMaxPointsInLeafNode(defaultMax, bytesPerDim, 1000);
        assertEquals(LEAF_SIZE_ALIGNMENT, result);
        assertEquals(0, result % LEAF_SIZE_ALIGNMENT);
    }

    public void testLargeFieldIncreasesLeafSize() {
        int defaultMax = BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE;
        int bytesPerDim = 8;
        int estimatedBytesPerLeaf = ESTIMATED_OVERHEAD_PER_LEAF_EXCLUDING_SPLIT_VALUES + bytesPerDim;
        long maxLeaves = TARGET_MAX_BKD_HEAP_BYTES / estimatedBytesPerLeaf;
        long pointCount = maxLeaves * defaultMax * 4;
        int result = adjustMaxPointsInLeafNode(defaultMax, bytesPerDim, pointCount);
        assertTrue(result > defaultMax);
        assertTrue(result <= MAX_POINTS_IN_LEAF_NODE_UPPER_BOUND);
        assertEquals(0, result % LEAF_SIZE_ALIGNMENT);
    }

    public void testUpperBoundIsRespected() {
        int defaultMax = BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE;
        int bytesPerDim = 1;
        long pointCount = Long.MAX_VALUE / 2;
        int result = adjustMaxPointsInLeafNode(defaultMax, bytesPerDim, pointCount);
        assertTrue(result <= MAX_POINTS_IN_LEAF_NODE_UPPER_BOUND);
        assertEquals(0, result % LEAF_SIZE_ALIGNMENT);
    }

    public void testUpperBoundIsAligned() {
        assertEquals(0, MAX_POINTS_IN_LEAF_NODE_UPPER_BOUND % LEAF_SIZE_ALIGNMENT);
    }

    public void testExactMultipleOfAlignmentUnchanged() {
        int result = adjustMaxPointsInLeafNode(1024, 8, 100);
        assertEquals(1024, result);
    }

    public void testZeroPointCountUsesDefault() {
        int defaultMax = BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE;
        int result = adjustMaxPointsInLeafNode(defaultMax, 8, 0);
        assertEquals(LEAF_SIZE_ALIGNMENT, result);
    }
}
