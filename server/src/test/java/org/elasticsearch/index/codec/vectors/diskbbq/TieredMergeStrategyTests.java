/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.vectors.diskbbq;

import org.elasticsearch.test.ESTestCase;

public class TieredMergeStrategyTests extends ESTestCase {

    public void testAllSmallSegmentsSelectsFullRebuild() {
        TieredMergeStrategy strategy = new TieredMergeStrategy(64, 128);
        int[] sizes = { 10, 20, 30 };
        int[] centroids = { 0, 0, 0 };
        assertEquals(TieredMergeStrategy.Strategy.FULL_REBUILD, strategy.selectStrategy(sizes, centroids));
    }

    public void testDominantSegmentSelectsInsertion() {
        TieredMergeStrategy strategy = new TieredMergeStrategy(64, 128);
        // One segment has 80% of vectors — insertion keeps dominant centroids with minimal iteration
        int[] sizes = { 8000, 500, 500, 500, 500 };
        int[] centroids = { 120, 8, 8, 8, 8 };
        assertEquals(TieredMergeStrategy.Strategy.INSERTION, strategy.selectStrategy(sizes, centroids));
    }

    public void testExactly70PercentSelectsConcatenation() {
        TieredMergeStrategy strategy = new TieredMergeStrategy(64, 128);
        int[] sizes = { 700, 300 };
        int[] centroids = { 10, 5 };
        // 700/1000 = 0.70, but total centroids < 32, so falls through to FULL_REBUILD
        assertEquals(TieredMergeStrategy.Strategy.FULL_REBUILD, strategy.selectStrategy(sizes, centroids));
    }

    public void testMultipleLargeSegmentsWithPriorCentroidsSelectsConcatenation() {
        TieredMergeStrategy strategy = new TieredMergeStrategy(64, 128);
        // Two roughly equal segments with prior centroids
        int[] sizes = { 3000, 4000 };
        int[] centroids = { 20, 20 };
        assertEquals(TieredMergeStrategy.Strategy.CONCATENATION, strategy.selectStrategy(sizes, centroids));
    }

    public void testInsufficientCentroidsSelectsFullRebuild() {
        TieredMergeStrategy strategy = new TieredMergeStrategy(64, 128);
        int[] sizes = { 3000, 4000 };
        int[] centroids = { 10, 10 };
        // total centroids = 20 < MIN_CENTROIDS_FOR_WARMSTART (32)
        assertEquals(TieredMergeStrategy.Strategy.FULL_REBUILD, strategy.selectStrategy(sizes, centroids));
    }

    public void testFindDominantSegment() {
        int[] sizes = { 100, 5000, 200, 300 };
        assertEquals(1, TieredMergeStrategy.findDominantSegment(sizes));
    }

    public void testFindDominantSegmentFirst() {
        int[] sizes = { 9999, 100, 200 };
        assertEquals(0, TieredMergeStrategy.findDominantSegment(sizes));
    }

    public void testFindDominantSegmentLast() {
        int[] sizes = { 100, 200, 9999 };
        assertEquals(2, TieredMergeStrategy.findDominantSegment(sizes));
    }

    public void testSingleSegmentSelectsInsertion() {
        TieredMergeStrategy strategy = new TieredMergeStrategy(64, 128);
        int[] sizes = { 10000 };
        int[] centroids = { 150 };
        // Single segment with enough centroids → insertion (100% dominant)
        assertEquals(TieredMergeStrategy.Strategy.INSERTION, strategy.selectStrategy(sizes, centroids));
    }

    public void testVerySmallTotalVectorsSelectsFullRebuild() {
        TieredMergeStrategy strategy = new TieredMergeStrategy(64, 128);
        // total = 100 < 64*4 = 256
        int[] sizes = { 40, 30, 30 };
        int[] centroids = { 50, 50, 50 };
        assertEquals(TieredMergeStrategy.Strategy.FULL_REBUILD, strategy.selectStrategy(sizes, centroids));
    }

    public void testLowDimLargeScaleDominantSelectsConcatenation() {
        TieredMergeStrategy strategy = new TieredMergeStrategy(64, 128);
        int[] sizes = { 70000, 30000 };
        int[] centroids = { 200, 50 };
        assertEquals(TieredMergeStrategy.Strategy.CONCATENATION, strategy.selectStrategy(sizes, centroids));
    }

    public void testHighDimLargeScaleDominantSelectsConcatenation() {
        TieredMergeStrategy strategy = new TieredMergeStrategy(64, 512);
        int[] sizes = { 70000, 30000 };
        int[] centroids = { 200, 50 };
        assertEquals(TieredMergeStrategy.Strategy.CONCATENATION, strategy.selectStrategy(sizes, centroids));
    }

    public void testHighDimModerateDominantSelectsInsertion() {
        TieredMergeStrategy strategy = new TieredMergeStrategy(64, 384);
        int[] sizes = { 40000, 10000 };
        int[] centroids = { 150, 30 };
        // 40000/50000 = 0.8, exactly at threshold → insertion
        assertEquals(TieredMergeStrategy.Strategy.INSERTION, strategy.selectStrategy(sizes, centroids));
    }

    public void testDominantBelowThresholdSelectsConcatenation() {
        TieredMergeStrategy strategy = new TieredMergeStrategy(64, 128);
        // 7500/10000 = 0.75, below insertion threshold → concatenation
        int[] sizes = { 7500, 1000, 1000, 500 };
        int[] centroids = { 100, 8, 8, 8 };
        assertEquals(TieredMergeStrategy.Strategy.CONCATENATION, strategy.selectStrategy(sizes, centroids));
    }

    public void testDominantWithInsufficientCentroidsSelectsConcatenation() {
        TieredMergeStrategy strategy = new TieredMergeStrategy(64, 128);
        // Dominant ratio 0.9 but dominant segment has only 20 centroids (<32) → falls to concatenation via totalCentroids
        int[] sizes = { 9000, 1000 };
        int[] centroids = { 20, 15 };
        assertEquals(TieredMergeStrategy.Strategy.CONCATENATION, strategy.selectStrategy(sizes, centroids));
    }

    public void testHighlyDominantSelectsInsertion() {
        TieredMergeStrategy strategy = new TieredMergeStrategy(64, 128);
        // 95% dominant ratio
        int[] sizes = { 95000, 5000 };
        int[] centroids = { 400, 20 };
        assertEquals(TieredMergeStrategy.Strategy.INSERTION, strategy.selectStrategy(sizes, centroids));
    }
}
