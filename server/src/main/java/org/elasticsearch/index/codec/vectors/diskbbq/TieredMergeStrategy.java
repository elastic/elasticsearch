/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.elasticsearch.index.codec.vectors.cluster.ClusteringFloatVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.ConcatenatedClusteringFloatVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans;
import org.elasticsearch.index.codec.vectors.cluster.KMeansResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Selects the optimal merge strategy based on input segment characteristics and returns
 * a {@link MergeAction} that encapsulates both the decision and the data needed to execute it.
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

    public TieredMergeStrategy(int vectorsPerCluster) {
        this.vectorsPerCluster = vectorsPerCluster;
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
     * A merge action that encapsulates the selected strategy and the data needed to execute it.
     * Call {@link #execute} with a pre-configured {@link HierarchicalKMeans} instance.
     */
    public sealed interface MergeAction {
        // Selected strategy
        Strategy strategy();

        // Execute the merge action, returning the clustering result
        KMeansResult<float[]> execute(HierarchicalKMeans<float[]> kmeans, ClusteringFloatVectorValues vectors, int vectorsPerCluster)
            throws IOException;
    }

    public record FullRebuild() implements MergeAction {
        @Override
        public Strategy strategy() {
            return Strategy.FULL_REBUILD;
        }

        @Override
        public KMeansResult<float[]> execute(HierarchicalKMeans<float[]> kmeans, ClusteringFloatVectorValues vectors, int vectorsPerCluster)
            throws IOException {
            return kmeans.cluster(vectors, vectorsPerCluster);
        }
    }

    public record Insertion(ClusteringFloatVectorValues seedCentroids) implements MergeAction {
        @Override
        public Strategy strategy() {
            return Strategy.INSERTION;
        }

        @Override
        public KMeansResult<float[]> execute(HierarchicalKMeans<float[]> kmeans, ClusteringFloatVectorValues vectors, int vectorsPerCluster)
            throws IOException {
            return kmeans.clusterByInsertion(vectors, seedCentroids, vectorsPerCluster);
        }
    }

    /**
     * Concatenation merge action.
     *
     * @param seedCentroids      prior centroids concatenated from segments that provided them
     * @param clusterSizes       per-prior-centroid vector counts (aligned with {@code seedCentroids})
     * @param coveredVectorCount sum of {@code segmentSizes[i]} for segments that contributed priors;
     *                           may be less than the total merged vector count when some input
     *                           segments do not surface prior centroids (e.g. 9.2/ES940 indices).
     *                           Used by {@link HierarchicalKMeans#clusterByConcatenation} to size
     *                           {@code k} without biasing toward the covered portion's density.
     */
    public record Concatenation(ClusteringFloatVectorValues seedCentroids, int[] clusterSizes, int coveredVectorCount)
        implements
            MergeAction {
        @Override
        public Strategy strategy() {
            return Strategy.CONCATENATION;
        }

        @Override
        public KMeansResult<float[]> execute(HierarchicalKMeans<float[]> kmeans, ClusteringFloatVectorValues vectors, int vectorsPerCluster)
            throws IOException {
            return kmeans.clusterByConcatenation(vectors, seedCentroids, clusterSizes, coveredVectorCount, vectorsPerCluster);
        }
    }

    /**
     * Select the merge action for the given segment characteristics.
     * The returned action encapsulates both the strategy decision and the centroid data
     * needed to execute it.
     *
     * @param segmentSizes     number of vectors per input segment
     * @param segmentCentroids number of centroids per input segment (0 if unavailable)
     * @param centroidData     per-segment centroid data (may contain nulls for segments without centroids)
     * @return a merge action ready to execute
     */
    public MergeAction selectAction(int[] segmentSizes, int[] segmentCentroids, IVFVectorsReader.CentroidData[] centroidData) {
        Strategy strategy = selectStrategy(segmentSizes, segmentCentroids);
        return switch (strategy) {
            case INSERTION -> {
                int dominantIdx = findDominantSegment(segmentSizes);
                yield new Insertion(centroidData[dominantIdx].centroids());
            }
            case CONCATENATION -> {
                List<ClusteringFloatVectorValues> parts = new ArrayList<>();
                List<int[]> sizesParts = new ArrayList<>();
                int totalSizes = 0;
                int coveredVectorCount = 0;
                for (int i = 0; i < centroidData.length; i++) {
                    IVFVectorsReader.CentroidData data = centroidData[i];
                    if (data != null) {
                        parts.add(data.centroids());
                        sizesParts.add(data.clusterSizes());
                        totalSizes += data.clusterSizes().length;
                        coveredVectorCount += segmentSizes[i];
                    }
                }
                int[] allClusterSizes = new int[totalSizes];
                int off = 0;
                for (int[] s : sizesParts) {
                    System.arraycopy(s, 0, allClusterSizes, off, s.length);
                    off += s.length;
                }
                ClusteringFloatVectorValues concatenated = new ConcatenatedClusteringFloatVectorValues(
                    parts.toArray(new ClusteringFloatVectorValues[0])
                );
                yield new Concatenation(concatenated, allClusterSizes, coveredVectorCount);
            }
            case FULL_REBUILD -> new FullRebuild();
        };
    }

    /**
     * Determine the best merge strategy for the given segment characteristics.
     *
     * @param segmentSizes     number of vectors per input segment
     * @param segmentCentroids number of centroids per input segment (0 if unavailable)
     * @return the selected strategy
     */
    Strategy selectStrategy(int[] segmentSizes, int[] segmentCentroids) {
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
    static int findDominantSegment(int[] segmentSizes) {
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
