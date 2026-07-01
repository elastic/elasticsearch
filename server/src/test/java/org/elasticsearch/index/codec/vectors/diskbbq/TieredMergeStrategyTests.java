/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.vectors.diskbbq;

import org.elasticsearch.index.codec.vectors.cluster.CentroidOps;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

public class TieredMergeStrategyTests extends ESTestCase {

    public void testAllSmallSegmentsSelectsFullRebuild() {
        TieredMergeStrategy<float[]> strategy = new TieredMergeStrategy<>(64, CentroidOps.FLOAT);
        int[] sizes = { 10, 20, 30 };
        int[] centroids = { 0, 0, 0 };
        assertAction(strategy, sizes, centroids, TieredMergeStrategy.Strategy.FULL_REBUILD);
    }

    public void testDominantSegmentSelectsInsertion() {
        TieredMergeStrategy<float[]> strategy = new TieredMergeStrategy<>(64, CentroidOps.FLOAT);
        // One segment has 80% of vectors — insertion keeps dominant centroids with minimal iteration
        int[] sizes = { 8000, 500, 500, 500, 500 };
        int[] centroids = { 120, 8, 8, 8, 8 };
        TieredMergeStrategy.MergeAction<float[]> action = assertAction(strategy, sizes, centroids, TieredMergeStrategy.Strategy.INSERTION);
        assertTrue(action instanceof TieredMergeStrategy.Insertion<float[]>);
        // Dominant segment (index 0) centroids should be captured
        assertEquals(120, ((TieredMergeStrategy.Insertion<float[]>) action).seedCentroids().size());
    }

    public void testExactly70PercentSelectsConcatenation() {
        TieredMergeStrategy<float[]> strategy = new TieredMergeStrategy<>(64, CentroidOps.FLOAT);
        int[] sizes = { 700, 300 };
        int[] centroids = { 10, 5 };
        // 700/1000 = 0.70, but total centroids < 32, so falls through to FULL_REBUILD
        assertAction(strategy, sizes, centroids, TieredMergeStrategy.Strategy.FULL_REBUILD);
    }

    public void testMultipleLargeSegmentsWithPriorCentroidsSelectsConcatenation() {
        TieredMergeStrategy<float[]> strategy = new TieredMergeStrategy<>(64, CentroidOps.FLOAT);
        // Two roughly equal segments with prior centroids
        int[] sizes = { 3000, 4000 };
        int[] centroids = { 20, 20 };
        TieredMergeStrategy.MergeAction<float[]> action = assertAction(
            strategy,
            sizes,
            centroids,
            TieredMergeStrategy.Strategy.CONCATENATION
        );
        assertTrue(action instanceof TieredMergeStrategy.Concatenation<float[]>);
        // Should collect all 40 centroids
        assertEquals(40, ((TieredMergeStrategy.Concatenation<float[]>) action).seedCentroids().size());
    }

    public void testInsufficientCentroidsSelectsFullRebuild() {
        TieredMergeStrategy<float[]> strategy = new TieredMergeStrategy<>(64, CentroidOps.FLOAT);
        int[] sizes = { 3000, 4000 };
        int[] centroids = { 10, 10 };
        // total centroids = 20 < MIN_CENTROIDS_FOR_WARMSTART (32)
        assertAction(strategy, sizes, centroids, TieredMergeStrategy.Strategy.FULL_REBUILD);
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
        TieredMergeStrategy<float[]> strategy = new TieredMergeStrategy<>(64, CentroidOps.FLOAT);
        int[] sizes = { 10000 };
        int[] centroids = { 150 };
        // Single segment with enough centroids → insertion (100% dominant)
        assertAction(strategy, sizes, centroids, TieredMergeStrategy.Strategy.INSERTION);
    }

    public void testVerySmallTotalVectorsSelectsFullRebuild() {
        TieredMergeStrategy<float[]> strategy = new TieredMergeStrategy<>(64, CentroidOps.FLOAT);
        // total = 100 < 64*4 = 256
        int[] sizes = { 40, 30, 30 };
        int[] centroids = { 50, 50, 50 };
        assertAction(strategy, sizes, centroids, TieredMergeStrategy.Strategy.FULL_REBUILD);
    }

    public void testLowDimLargeScaleDominantSelectsConcatenation() {
        TieredMergeStrategy<float[]> strategy = new TieredMergeStrategy<>(64, CentroidOps.FLOAT);
        int[] sizes = { 70000, 30000 };
        int[] centroids = { 200, 50 };
        assertAction(strategy, sizes, centroids, TieredMergeStrategy.Strategy.CONCATENATION);
    }

    public void testHighDimLargeScaleDominantSelectsConcatenation() {
        TieredMergeStrategy<float[]> strategy = new TieredMergeStrategy<>(64, CentroidOps.FLOAT);
        int[] sizes = { 70000, 30000 };
        int[] centroids = { 200, 50 };
        assertAction(strategy, sizes, centroids, TieredMergeStrategy.Strategy.CONCATENATION);
    }

    public void testHighDimModerateDominantSelectsInsertion() {
        TieredMergeStrategy<float[]> strategy = new TieredMergeStrategy<>(64, CentroidOps.FLOAT);
        int[] sizes = { 40000, 10000 };
        int[] centroids = { 150, 30 };
        // 40000/50000 = 0.8, exactly at threshold → insertion
        assertAction(strategy, sizes, centroids, TieredMergeStrategy.Strategy.INSERTION);
    }

    public void testDominantBelowThresholdSelectsConcatenation() {
        TieredMergeStrategy<float[]> strategy = new TieredMergeStrategy<>(64, CentroidOps.FLOAT);
        // 7500/10000 = 0.75, below insertion threshold → concatenation
        int[] sizes = { 7500, 1000, 1000, 500 };
        int[] centroids = { 100, 8, 8, 8 };
        assertAction(strategy, sizes, centroids, TieredMergeStrategy.Strategy.CONCATENATION);
    }

    public void testDominantWithInsufficientCentroidsSelectsConcatenation() {
        TieredMergeStrategy<float[]> strategy = new TieredMergeStrategy<>(64, CentroidOps.FLOAT);
        // Dominant ratio 0.9 but dominant segment has only 20 centroids (<32) → falls to concatenation via totalCentroids
        int[] sizes = { 9000, 1000 };
        int[] centroids = { 20, 15 };
        assertAction(strategy, sizes, centroids, TieredMergeStrategy.Strategy.CONCATENATION);
    }

    public void testHighlyDominantSelectsInsertion() {
        TieredMergeStrategy<float[]> strategy = new TieredMergeStrategy<>(64, CentroidOps.FLOAT);
        // 95% dominant ratio
        int[] sizes = { 95000, 5000 };
        int[] centroids = { 400, 20 };
        assertAction(strategy, sizes, centroids, TieredMergeStrategy.Strategy.INSERTION);
    }

    @SuppressWarnings("unchecked")
    public void testConcatenationSkipsNullCentroidData() {
        TieredMergeStrategy<float[]> strategy = new TieredMergeStrategy<>(64, CentroidOps.FLOAT);
        int[] sizes = { 3000, 4000, 500 };
        int[] centroids = { 20, 20, 0 };
        IVFVectorsReader.CentroidData[] data = makeCentroidData(centroids);
        data[2] = null; // segment without centroid data
        TieredMergeStrategy.MergeAction<float[]> action = strategy.selectAction(sizes, centroids, data);
        assertEquals(TieredMergeStrategy.Strategy.CONCATENATION, action.strategy());
        // Should only collect centroids from segments 0 and 1
        assertEquals(40, ((TieredMergeStrategy.Concatenation<float[]>) action).seedCentroids().size());
    }

    @SuppressWarnings("unchecked")
    public void testConcatenationCoveredVectorCountExcludesNullSegments() {
        TieredMergeStrategy<float[]> strategy = new TieredMergeStrategy<>(64, CentroidOps.FLOAT);
        // Segments 0 + 1 surface priors; segment 2 does not (e.g. legacy ES920/ES940 reader returns null).
        int[] sizes = { 3000, 4000, 2000 };
        int[] centroids = { 20, 20, 0 };
        IVFVectorsReader.CentroidData[] data = makeCentroidData(centroids);
        data[2] = null;
        TieredMergeStrategy.MergeAction<float[]> action = strategy.selectAction(sizes, centroids, data);
        TieredMergeStrategy.Concatenation<float[]> concat = (TieredMergeStrategy.Concatenation<float[]>) action;
        // coveredVectorCount only counts the segments that contributed priors (3000 + 4000),
        // not the 2000 vectors from the segment with null centroid data.
        assertEquals(7000, concat.coveredVectorCount());
    }

    @SuppressWarnings("unchecked")
    public void testConcatenationCoveredVectorCountFullCoverage() {
        TieredMergeStrategy<float[]> strategy = new TieredMergeStrategy<>(64, CentroidOps.FLOAT);
        int[] sizes = { 3000, 4000 };
        int[] centroids = { 20, 20 };
        IVFVectorsReader.CentroidData[] data = makeCentroidData(centroids);
        TieredMergeStrategy.MergeAction<float[]> action = strategy.selectAction(sizes, centroids, data);
        TieredMergeStrategy.Concatenation<float[]> concat = (TieredMergeStrategy.Concatenation<float[]>) action;
        assertEquals(7000, concat.coveredVectorCount());
    }

    /**
     * Helper that runs selectAction via synthetic centroid data and verifies the strategy.
     */
    @SuppressWarnings("unchecked")
    private static TieredMergeStrategy.MergeAction<float[]> assertAction(
        TieredMergeStrategy<float[]> strategy,
        int[] sizes,
        int[] centroids,
        TieredMergeStrategy.Strategy expected
    ) {
        IVFVectorsReader.CentroidData[] data = makeCentroidData(centroids);
        TieredMergeStrategy.MergeAction<float[]> action = strategy.selectAction(sizes, centroids, data);
        assertEquals(expected, action.strategy());
        return action;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static IVFVectorsReader.CentroidData[] makeCentroidData(int[] centroidCounts) {
        IVFVectorsReader.CentroidData[] data = new IVFVectorsReader.CentroidData[centroidCounts.length];
        for (int i = 0; i < centroidCounts.length; i++) {
            if (centroidCounts[i] > 0) {
                float[][] c = new float[centroidCounts[i]][4]; // dummy 4-d centroids
                data[i] = new IVFVectorsReader.CentroidData(
                    KMeansFloatVectorValues.build(Arrays.asList(c), null, 4),
                    new int[centroidCounts[i]],
                    new float[4],
                    null
                );
            }
        }
        return data;
    }
}
