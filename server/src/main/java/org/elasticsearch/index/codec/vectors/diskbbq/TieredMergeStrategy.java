/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

/**
 * Selects the optimal merge strategy based on input segment characteristics.
 *
 * <table>
 * <caption>Strategy selection matrix</caption>
 * <tr><th>Scenario</th><th>Strategy</th></tr>
 * <tr><td>Total vectors very small</td><td>FULL_REBUILD</td></tr>
 * <tr><td>Dominant segment ≥80% with enough centroids</td><td>INSERTION</td></tr>
 * <tr><td>Segments with enough prior centroids</td><td>CONCATENATION</td></tr>
 * <tr><td>Default</td><td>FULL_REBUILD</td></tr>
 * </table>
 */
public class TieredMergeStrategy {

    /**
     * Minimum total centroids across all segments to justify concatenation.
     * Below this, full rebuild is cheap enough.
     */
    static final int MIN_CENTROIDS_FOR_CONCATENATION = 32;

    /**
     * Minimum dominant segment ratio for INSERTION strategy.
     * When a single segment contains at least this fraction of total vectors,
     * its centroids are a good enough representation to skip iterative convergence.
     */
    static final float INSERTION_DOMINANT_RATIO = 0.8f;

    private final int vectorsPerCluster;
    private final int dimension;

    public TieredMergeStrategy(int vectorsPerCluster, int dimension) {
        this.vectorsPerCluster = vectorsPerCluster;
        this.dimension = dimension;
    }

    public enum Strategy {
        /** Current behavior: full hierarchical K-means from scratch */
        FULL_REBUILD,
        /** Concatenate prior centroids, reduce to target K, single assignment pass (no iterative convergence) */
        CONCATENATION,
        /** HNSW-style insertion: keep dominant segment's clustering, assign new vectors with minimal iteration */
        INSERTION
    }

    /**
     * Determine the best merge strategy for the given segment characteristics.
     *
     * @param segmentSizes     number of vectors per input segment
     * @param segmentCentroids number of centroids per input segment (0 if unavailable)
     * @return the selected strategy
     */
    public Strategy selectStrategy(int[] segmentSizes, int[] segmentCentroids) {
        int totalVectors = 0;
        int maxSegmentSize = 0;
        int totalCentroids = 0;

        for (int i = 0; i < segmentSizes.length; i++) {
            totalVectors += segmentSizes[i];
            if (segmentSizes[i] > maxSegmentSize) {
                maxSegmentSize = segmentSizes[i];
            }
            totalCentroids += segmentCentroids[i];
        }

        // If total vectors is very small, full rebuild is cheap
        if (totalVectors < vectorsPerCluster * 4) {
            return Strategy.FULL_REBUILD;
        }

        // If a single segment dominates (≥80% of vectors) and has enough centroids,
        // use insertion: keep its centroids and assign new vectors with minimal iteration.
        // This is the K-means analog of HNSW's "pick biggest graph and insert."
        if (totalVectors > 0) {
            float dominantRatio = (float) maxSegmentSize / totalVectors;
            if (dominantRatio >= INSERTION_DOMINANT_RATIO) {
                int dominantIdx = findDominantSegment(segmentSizes);
                if (segmentCentroids[dominantIdx] >= MIN_CENTROIDS_FOR_CONCATENATION) {
                    return Strategy.INSERTION;
                }
            }
        }

        // If we have enough prior centroids from any segments, concatenate them and do
        // a single assignment pass. This avoids iterative convergence overhead since
        // the prior centroids are already good approximations of the data distribution.
        if (totalCentroids >= MIN_CENTROIDS_FOR_CONCATENATION) {
            return Strategy.CONCATENATION;
        }

        return Strategy.FULL_REBUILD;
    }

    /**
     * Find the index of the dominant (largest) segment.
     *
     * @param segmentSizes number of vectors per input segment
     * @return index of the segment with the most vectors
     */
    public static int findDominantSegment(int[] segmentSizes) {
        int maxIdx = 0;
        int maxSize = segmentSizes[0];
        for (int i = 1; i < segmentSizes.length; i++) {
            if (segmentSizes[i] > maxSize) {
                maxSize = segmentSizes[i];
                maxIdx = i;
            }
        }
        return maxIdx;
    }
}
