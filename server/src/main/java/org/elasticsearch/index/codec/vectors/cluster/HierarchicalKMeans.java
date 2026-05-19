/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * An implementation of the hierarchical k-means algorithm that better partitions data than naive k-means
 */
public class HierarchicalKMeans {

    private static final Logger logger = LogManager.getLogger(HierarchicalKMeans.class);

    public static final int MAXK = 128;
    public static final int SAMPLES_PER_CLUSTER_DEFAULT = 64;
    public static final float DEFAULT_SOAR_LAMBDA = 1.0f;
    public static final int NO_SOAR_ASSIGNMENT = -1;
    private static final int MIN_VECTORS_PRE_THREAD = 64;

    public static final boolean USE_BALANCING = true;
    public static final int MAX_ITERATIONS_DEFAULT = USE_BALANCING ? 2 : 6;

    // Hyperparameter for capacity-constrained assignment in reduceVarianceAware
    // Allowed overflow above ceil(totalVectors / k) per meta-cluster. Tighter values produce
    // more balanced output sizes; looser values preserve geometric locality.
    static final float CAPACITY_SLACK = 0.25f;

    // Threshold in resolveOversizedAndEmptyClusters above which a cluster is recursively split.
    // count > OVERSIZED_THRESHOLD * targetSize triggers a split. Smaller values produce more
    // (smaller) final centroids — pushes total count up and per-cluster size down.
    static final float OVERSIZED_THRESHOLD = 1.0f;

    final int dimension;
    final int maxIterations;
    final int samplesPerCluster;
    final int clustersPerNeighborhood;
    final float soarLambda;

    private final TaskExecutor executor;
    private final int numWorkers;

    private HierarchicalKMeans(
        int dimension,
        TaskExecutor executor,
        int numWorkers,
        int maxIterations,
        int samplesPerCluster,
        int clustersPerNeighborhood,
        float soarLambda
    ) {
        this.dimension = dimension;
        this.executor = executor;
        this.numWorkers = numWorkers;
        this.maxIterations = maxIterations;
        this.samplesPerCluster = samplesPerCluster;
        this.clustersPerNeighborhood = clustersPerNeighborhood;
        this.soarLambda = soarLambda;
    }

    public static HierarchicalKMeans ofSerial(int dimension) {
        return ofSerial(dimension, MAX_ITERATIONS_DEFAULT, SAMPLES_PER_CLUSTER_DEFAULT, MAXK, DEFAULT_SOAR_LAMBDA);
    }

    public static HierarchicalKMeans ofSerial(
        int dimension,
        int maxIterations,
        int samplesPerCluster,
        int clustersPerNeighborhood,
        float soarLambda
    ) {
        return new HierarchicalKMeans(dimension, null, 1, maxIterations, samplesPerCluster, clustersPerNeighborhood, soarLambda);
    }

    public static HierarchicalKMeans ofConcurrent(int dimension, TaskExecutor executor, int numWorkers) {
        return ofConcurrent(
            dimension,
            executor,
            numWorkers,
            MAX_ITERATIONS_DEFAULT,
            SAMPLES_PER_CLUSTER_DEFAULT,
            MAXK,
            DEFAULT_SOAR_LAMBDA
        );
    }

    public static HierarchicalKMeans ofConcurrent(
        int dimension,
        TaskExecutor executor,
        int numWorkers,
        int maxIterations,
        int samplesPerCluster,
        int clustersPerNeighborhood,
        float soarLambda
    ) {
        return new HierarchicalKMeans(
            dimension,
            executor,
            numWorkers,
            maxIterations,
            samplesPerCluster,
            clustersPerNeighborhood,
            soarLambda
        );

    }

    /**
     * clusters the set of vectors by starting with a rough number of partitions and then recursively refining those
     * lastly a pass is made to adjust nearby neighborhoods and add an extra assignment per vector to nearby neighborhoods
     *
     * @param vectors the vectors to cluster
     * @param targetSize the rough number of vectors that should be attached to a cluster
     * @return the centroids and the vectors assignments and SOAR (spilled from nearby neighborhoods) assignments
     * @throws IOException is thrown if vectors is inaccessible
     */
    public KMeansResult cluster(ClusteringFloatVectorValues vectors, int targetSize) throws IOException {
        if (vectors.size() == 0) {
            return new KMeansIntermediate();
        }

        // if we have a small number of vectors calculate the centroid directly
        if (vectors.size() <= targetSize) {
            float[] centroid = new float[dimension];
            // sum the vectors
            for (int i = 0; i < vectors.size(); i++) {
                float[] vector = vectors.vectorValue(i);
                for (int j = 0; j < dimension; j++) {
                    centroid[j] += vector[j];
                }
            }
            // average the vectors
            for (int j = 0; j < dimension; j++) {
                centroid[j] /= vectors.size();
            }
            return new KMeansIntermediate(new float[][] { centroid }, new int[vectors.size()]);
        }

        // partition the space
        KMeansIntermediate kMeansIntermediate = clusterAndSplit(vectors, targetSize);

        if (kMeansIntermediate.centroids().length > 1 && kMeansIntermediate.centroids().length < vectors.size()) {
            int localSampleSize = Math.min(kMeansIntermediate.centroids().length * samplesPerCluster / 2, vectors.size());
            KMeansLocal kMeansLocal = buildKmeansLocalFinal(vectors.size(), localSampleSize);
            kMeansLocal.cluster(vectors, kMeansIntermediate, clustersPerNeighborhood, soarLambda);
        }
        return kMeansIntermediate;
    }

    private KMeansIntermediate clusterAndSplit(final ClusteringFloatVectorValues vectors, final int targetSize) throws IOException {
        if (vectors.size() <= targetSize) {
            return new KMeansIntermediate();
        }

        int k = Math.clamp((int) ((vectors.size() + targetSize / 2.0f) / (float) targetSize), 2, MAXK);
        int m = Math.min(k * samplesPerCluster, vectors.size());

        // TODO: instead of creating a sub-cluster assignments reuse the parent array each time
        int[] assignments = new int[vectors.size()];
        // ensure we don't over assign to cluster 0 without adjusting it
        Arrays.fill(assignments, -1);
        float[][] centroids = LloydKMeansLocal.pickInitialCentroids(vectors, k);
        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(centroids, assignments, vectors::ordToDoc);
        KMeansLocal kMeansLocal = buildKmeansLocal(vectors.size(), m);
        kMeansLocal.cluster(vectors, kMeansIntermediate);

        centroids = kMeansIntermediate.centroids();
        int[] centroidVectorCount = kMeansIntermediate.clusterCounts();

        if (centroids.length == 1) {
            return kMeansIntermediate;
        }

//<<<<<<< reduce-diskbbq-merge-cost-tiered-strategy
        resolveOversizedAndEmptyClusters(kMeansIntermediate, vectors, centroidVectorCount, targetSize);
//=======
        int centroidIndexOffset = 0; // tracks the cumulative change in centroid indices due to splits and removals
        for (int c = 0; c < centroidVectorCount.length; c++) {
            // Recurse for each cluster which is larger than targetSize
            final int count = centroidVectorCount[c];
            final int adjustedCentroid = c + centroidIndexOffset;
            if (count > targetSize) {
                final ClusteringFloatVectorValues sample = createClusterSlice(count, adjustedCentroid, vectors, assignments);

                // TODO: consider iterative here instead of recursive
                // recursive call to build out the sub partitions around this centroid c
                // subsequently reconcile and flatten the space of all centroids and assignments into one structure we can return
                KMeansIntermediate subPartitions = clusterAndSplit(sample, targetSize);
                // Update offset: split replaces 1 centroid with subPartitions.centroids().length centroids
                centroidIndexOffset += updateAssignmentsWithRecursiveSplit(kMeansIntermediate, adjustedCentroid, subPartitions);
            }
        }
//>>>>>>> main

        return kMeansIntermediate;
    }

    private KMeansLocal buildKmeansLocal(int numVectors, int localSampleSize) {
        return buildKmeansLocal(numVectors, localSampleSize, maxIterations);
    }

    private KMeansLocal buildKmeansLocal(int numVectors, int localSampleSize, int iterations) {
        int numWorkers = Math.min(this.numWorkers, numVectors / MIN_VECTORS_PRE_THREAD);
        // if there is no executor or there is no enough vectors for more than one thread, use the serial version
        if (USE_BALANCING) {
            return executor == null || numWorkers <= 1
                ? new BalancedOTKMeansLocalSerial(localSampleSize, iterations)
                : new BalancedOTKMeansLocalConcurrent(executor, numWorkers, localSampleSize, iterations);
        } else {
            return executor == null || numWorkers <= 1
                ? new LloydKMeansLocalSerial(localSampleSize, iterations)
                : new LloydKMeansLocalConcurrent(executor, numWorkers, localSampleSize, iterations);
        }
    }

    private KMeansLocal buildKmeansLocalFinal(int numVectors, int localSampleSize) {
        int numWorkers = Math.min(this.numWorkers, numVectors / MIN_VECTORS_PRE_THREAD);
        // if there is no executor or there is no enough vectors for more than one thread, use the serial version
        if (USE_BALANCING) {
            return executor == null || numWorkers <= 1
                ? new BalancedASKMeansLocalSerial(localSampleSize, maxIterations)
                : new BalancedASKMeansLocalConcurrent(executor, numWorkers, localSampleSize, maxIterations);
        } else {
            return executor == null || numWorkers <= 1
                ? new LloydKMeansLocalSerial(localSampleSize, maxIterations)
                : new LloydKMeansLocalConcurrent(executor, numWorkers, localSampleSize, maxIterations);
        }
    }

    static ClusteringFloatVectorValues createClusterSlice(
        int clusterSize,
        int cluster,
        ClusteringFloatVectorValues vectors,
        int[] assignments
    ) {
        assert assignments.length == vectors.size();
        int[] slice = new int[clusterSize];
        int idx = 0;
        for (int i = 0; i < assignments.length; i++) {
            if (assignments[i] == cluster) {
                slice[idx] = i;
                idx++;
            }
        }
        assert idx == clusterSize;

        return new ClusteringFloatVectorValuesSlice(vectors, i -> slice[i], slice.length);
    }

    private static float[][] deepCopy(float[][] source) {
        float[][] copy = new float[source.length][];
        for (int i = 0; i < source.length; i++) {
            copy[i] = Arrays.copyOf(source[i], source[i].length);
        }
        return copy;
    }

    /**
     * Reduce a large set of centroids to targetCount using lightweight K-means.
     * Operates only on centroids (not full vectors), so it's very fast.
     */
//<<<<<<< reduce-diskbbq-merge-cost-tiered-strategy
    private float[][] reduceCentroids(float[][] centroids, int targetCount) throws IOException {
        ClusteringFloatVectorValues centroidValues = KMeansFloatVectorValues.build(java.util.Arrays.asList(centroids), null, dimension);
        float[][] reduced = KMeansLocal.pickInitialCentroids(centroidValues, targetCount);
        // Run a few Lloyd iterations to refine
        KMeansIntermediate intermediate = new KMeansIntermediate(reduced, new int[centroids.length], centroidValues::ordToDoc);
        KMeansLocal kMeansLocal = new LloydKMeansLocalSerial(centroids.length, 3);
        kMeansLocal.cluster(centroidValues, intermediate);
        return intermediate.centroids();
    }

    /**
     * Reduces N prior centroids to targetCount seed centroids for the concatenation merge path,
     * ensuring each meta-cluster represents roughly equal total vector mass.
     * <p>
     * Uses size-weighted k-means++ seeding (proportional to size × distance²) followed by a
     * single capacity-constrained greedy assignment pass — no iterative solver is run, keeping
     * the merge path fast.
     *
     * @param centroids    prior centroids from all input segments
     * @param clusterSizes number of vectors per centroid in the prior segments
     * @param targetCount  desired centroid count
     * @return reduced centroids
     */
    private float[][] reduceVarianceAware(float[][] centroids, int[] clusterSizes, int targetCount) {
        int n = centroids.length;
        int k = Math.min(targetCount, n);
        logger.debug("reduceVarianceAware: reducing [{}] centroids to [{}]", n, k);

        // Step 1: Size-weighted k-means++ seeds — each seed is sampled proportional to
        // size × distance², placing seeds in high-density regions so meta-clusters start
        // with roughly equal vector mass.
        float[][] seeds = sizeWeightedKMeansPlusPlusSeeds(centroids, clusterSizes, k);

        // Step 2: Capacity-constrained greedy assignment — single pass, no iteration.
        // Assigns centroids to their nearest seed while capping each meta-cluster at
        // ceil(totalVectors/k) + 25% slack, bounding output size variance.
        int[] assignments = capacityConstrainedAssign(centroids, clusterSizes, seeds, k);

        // Step 3: Compute size-weighted mean centroid for each meta-cluster.
        long[] totalSizes = new long[k];
        float[][] sums = new float[k][dimension];
        for (int i = 0; i < n; i++) {
            int c = assignments[i];
            totalSizes[c] += clusterSizes[i];
            for (int d = 0; d < dimension; d++) {
                sums[c][d] += (float) clusterSizes[i] * centroids[i][d];
            }
        }
        List<float[]> result = new ArrayList<>();
        for (int c = 0; c < k; c++) {
            if (totalSizes[c] > 0) {
                float[] centroid = new float[dimension];
                for (int d = 0; d < dimension; d++) {
                    centroid[d] = sums[c][d] / totalSizes[c];
                }
                result.add(centroid);
            }
        }
        logger.debug("reduceVarianceAware: output [{}] centroids", result.size());
        return result.toArray(new float[0][]);
    }

    /**
     * k-means++ seeding weighted by cluster size. The first seed is sampled proportional to
     * cluster size; each subsequent seed is sampled proportional to size × distance² to the
     * nearest already-selected seed. This places seeds in high-density regions so that each
     * resulting meta-cluster starts with roughly equal total vector mass.
     */
    private float[][] sizeWeightedKMeansPlusPlusSeeds(float[][] centroids, int[] clusterSizes, int k) {
        int n = centroids.length;
        k = Math.min(k, n);
        float[][] selected = new float[k][];
        Random random = new Random(42L);

        // First seed: proportional to cluster size, biasing toward high-density regions.
        long totalSize = 0;
        for (int s : clusterSizes)
            totalSize += s;
        double r = random.nextDouble() * totalSize;
        double cum = 0;
        int firstIdx = n - 1;
        for (int i = 0; i < n; i++) {
            cum += clusterSizes[i];
            if (cum > r) {
                firstIdx = i;
                break;
            }
        }
        selected[0] = Arrays.copyOf(centroids[firstIdx], centroids[firstIdx].length);

        float[] minDist = new float[n];
        Arrays.fill(minDist, Float.MAX_VALUE);

        // weights[j] = clusterSizes[j] * minDist[j]; cached to avoid recomputing in the selection pass.
        double[] weights = new double[n];
        for (int c = 1; c < k; c++) {
            double totalWeight = 0;
            for (int j = 0; j < n; j++) {
                float d = ESVectorUtil.squareDistance(centroids[j], selected[c - 1]);
                if (d < minDist[j]) {
                    minDist[j] = d;
                }
                weights[j] = (double) clusterSizes[j] * minDist[j];
                totalWeight += weights[j];
            }
            // rv requires totalWeight to be fully accumulated before sampling, so two passes are necessary.
            double rv = random.nextDouble() * totalWeight;
            double cumulative = 0;
            int chosen = n - 1;
            for (int j = 0; j < n; j++) {
                cumulative += weights[j];
                if (cumulative >= rv) {
                    chosen = j;
                    break;
                }
            }
            selected[c] = Arrays.copyOf(centroids[chosen], centroids[chosen].length);
        }
        return selected;
    }

    /**
     * Assigns each centroid to its nearest seed subject to a per-seed vector-count capacity of
     * ceil(totalVectors/k) + 25% slack. Centroids are processed in descending order of assignment
     * regret (gap between nearest and second-nearest seed distance) so the most constrained
     * assignments are resolved first. Falls back to the geometrically nearest seed if all seeds
     * are at capacity.
     */
    private int[] capacityConstrainedAssign(float[][] centroids, int[] clusterSizes, float[][] seeds, int k) {
        int n = centroids.length;

        long totalVectors = 0;
        for (int s : clusterSizes)
            totalVectors += s;
        long capacityPerSeed = (totalVectors + k - 1) / k;
        // Allow CAPACITY_SLACK overflow so minor seed-placement imbalances don't force long-range assignments.
        long maxCapacity = capacityPerSeed + (long) (capacityPerSeed * CAPACITY_SLACK);

        long[] remaining = new long[k];
        Arrays.fill(remaining, maxCapacity);

        // Precompute distances and nearest / 2nd-nearest seed for each centroid.
        float[][] dist = new float[n][k];
        int[] nearest = new int[n];
        float[] nearestDist = new float[n];
        float[] secondDist = new float[n];
        Arrays.fill(nearestDist, Float.MAX_VALUE);
        Arrays.fill(secondDist, Float.MAX_VALUE);
        for (int i = 0; i < n; i++) {
            for (int c = 0; c < k; c++) {
                float d = ESVectorUtil.squareDistance(centroids[i], seeds[c]);
                dist[i][c] = d;
                if (d < nearestDist[i]) {
                    secondDist[i] = nearestDist[i];
                    nearestDist[i] = d;
                    nearest[i] = c;
                } else if (d < secondDist[i]) {
                    secondDist[i] = d;
                }
            }
        }

        // Sort by descending regret so the centroid with the highest cost of displacement
        // is assigned to its preferred seed before that seed can fill up.
        Integer[] order = new Integer[n];
        for (int i = 0; i < n; i++)
            order[i] = i;
        Arrays.sort(order, (a, b) -> Float.compare(secondDist[b] - nearestDist[b], secondDist[a] - nearestDist[a]));

        int[] assignments = new int[n];
        for (int idx : order) {
            float bestDist = Float.MAX_VALUE;
            int bestSeed = -1;
            for (int c = 0; c < k; c++) {
                if (remaining[c] > 0 && dist[idx][c] < bestDist) {
                    bestDist = dist[idx][c];
                    bestSeed = c;
                }
            }
            int chosen = bestSeed != -1 ? bestSeed : nearest[idx];
            assignments[idx] = chosen;
            remaining[chosen] = Math.max(0, remaining[chosen] - clusterSizes[idx]);
        }

        return assignments;
    }

    private void assignSoarOnly(ClusteringFloatVectorValues vectors, KMeansIntermediate kMeansIntermediate) throws IOException {
        if (soarLambda < 0) {
            return;
        }

        NeighborHood[] neighborhoods = null;
        float[][] centroids = kMeansIntermediate.centroids();
        if (centroids.length > clustersPerNeighborhood) {
            neighborhoods = executor == null || numWorkers < 2
                ? NeighborHood.computeNeighborhoods(centroids, clustersPerNeighborhood)
                : NeighborHood.computeNeighborhoods(executor, numWorkers, centroids, clustersPerNeighborhood);
        }

        kMeansIntermediate.setSoarAssignments(new int[vectors.size()]);
        int effectiveWorkers = Math.min(numWorkers, vectors.size() / MIN_VECTORS_PRE_THREAD);
        if (executor != null && effectiveWorkers >= 2) {
            KMeansLocal.assignSpilledConcurrent(executor, effectiveWorkers, vectors, kMeansIntermediate, neighborhoods, soarLambda);
        } else {
            KMeansLocal.assignSpilledSlice(vectors, kMeansIntermediate, neighborhoods, soarLambda, 0, vectors.size());
        }
    }

    /**
     * Supplement a small set of centroids with random vectors to reach targetCount.
     * Uses stride-based sampling to spread selected vectors evenly across the dataset.
     */
    private static float[][] supplementCentroids(float[][] existing, ClusteringFloatVectorValues vectors, int targetCount)
        throws IOException {
        float[][] augmented = new float[targetCount][];
        for (int i = 0; i < existing.length; i++) {
            augmented[i] = Arrays.copyOf(existing[i], existing[i].length);
        }
        int needed = targetCount - existing.length;
        // Stride-based sampling: pick every (size/needed)-th vector for even distribution
        int filled = 0;
        double stride = (double) vectors.size() / needed;
        for (int i = 0; filled < needed && i < vectors.size(); i++) {
            if (i >= (long) (filled * stride)) {
                float[] vector = vectors.vectorValue(i);
                augmented[existing.length + filled] = Arrays.copyOf(vector, vector.length);
                filled++;
            }
        }
        // Fallback: fill any remaining slots from the start (shouldn't happen with stride sampling)
        for (int i = 0; filled < needed && i < vectors.size(); i++) {
            float[] vector = vectors.vectorValue(i);
            augmented[existing.length + filled] = Arrays.copyOf(vector, vector.length);
            filled++;
        }
        return augmented;
    }

    /**
     * Cluster vectors by inserting them into an existing centroid structure.
     * Analogous to HNSW merge where the biggest graph is kept and new vectors are inserted:
     * uses the provided centroids as a near-final clustering and does minimal iteration —
     * one assignment pass to place all vectors, then one refinement pass for neighborhood
     * awareness and SOAR. This is significantly faster when the initial centroids already
     * represent the majority of the data (e.g., from a dominant segment).
     *
     * @param vectors          the vectors to cluster
     * @param initialCentroids centroids from the dominant segment
     * @param targetSize       target number of vectors per cluster
     * @return clustering result with assignments and SOAR assignments
     */
    public KMeansResult clusterByInsertion(ClusteringFloatVectorValues vectors, float[][] initialCentroids, int targetSize)
        throws IOException {
        if (vectors.size() == 0) {
            return new KMeansIntermediate();
        }

        if (vectors.size() <= targetSize) {
            float[] centroid = new float[dimension];
            for (int i = 0; i < vectors.size(); i++) {
                float[] vector = vectors.vectorValue(i);
                for (int j = 0; j < dimension; j++) {
                    centroid[j] += vector[j];
                }
            }
            for (int j = 0; j < dimension; j++) {
                centroid[j] /= vectors.size();
            }
            return new KMeansIntermediate(new float[][] { centroid }, new int[vectors.size()]);
        }

        int k = Math.clamp((int) ((vectors.size() + targetSize / 2.0f) / (float) targetSize), 2, MAXK);

        float[][] seedCentroids;
        if (initialCentroids.length == k) {
            seedCentroids = deepCopy(initialCentroids);
        } else if (initialCentroids.length > k) {
            seedCentroids = reduceCentroids(initialCentroids, k);
        } else {
            seedCentroids = supplementCentroids(initialCentroids, vectors, k);
        }

        // Single assignment pass: assign all vectors to nearest seed centroid and update positions.
        // No sampling — with only 1 iteration, a full pass is both faster and more accurate than
        // a sampled pass followed by a final full pass.
        int[] assignments = new int[vectors.size()];
        Arrays.fill(assignments, -1);
        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(seedCentroids, assignments, vectors::ordToDoc);
        KMeansLocal kMeansLocal = buildKmeansLocal(vectors.size(), vectors.size(), 1);
        kMeansLocal.cluster(vectors, kMeansIntermediate);

        // Handle oversized clusters by recursive splitting, same as clusterAndSplit
        int[] centroidVectorCount = new int[seedCentroids.length];
        for (int assignment : assignments) {
            if (assignment >= 0) {
                centroidVectorCount[assignment]++;
            }
        }

        resolveOversizedAndEmptyClusters(kMeansIntermediate, vectors, centroidVectorCount, targetSize);

        // Neighborhood-aware refinement + SOAR with minimal iteration.
        // One pass is sufficient since the initial assignment is already good.
        if (kMeansIntermediate.centroids().length > 1 && kMeansIntermediate.centroids().length < vectors.size()) {
            KMeansLocal refinementKMeans = buildKmeansLocal(vectors.size(), vectors.size(), 1);
            refinementKMeans.cluster(vectors, kMeansIntermediate, clustersPerNeighborhood, soarLambda);
        }

        return kMeansIntermediate;
    }

    /**
     * Cluster vectors by concatenating prior centroids and doing a single assignment pass.
     * <p>
     * For merges of balanced segments (no dominant segment) where prior centroids exist:
     * concatenate all prior centroids, reduce/expand to target K, then do a single full
     * assignment pass + SOAR. This eliminates iterative convergence entirely — the prior
     * centroids are already good approximations of the data distribution.
     * <p>
     * Reduction uses size-weighted k-means++ seeding plus a capacity-constrained greedy
     * assignment (see {@link #reduceVarianceAware}), bounding meta-cluster size variance
     * without running a full balancing solver.
     *
     * @param vectors           the merged vectors
     * @param allPriorCentroids centroids concatenated from all input segments
     * @param clusterSizes      number of vectors per centroid in the prior segments
     * @param targetSize        target vectors per cluster
     * @return clustering result
     */
    public KMeansResult clusterByConcatenation(
        ClusteringFloatVectorValues vectors,
        float[][] allPriorCentroids,
        int[] clusterSizes,
        int targetSize
    ) throws IOException {
        if (vectors.size() == 0) {
            return new KMeansIntermediate();
        }
        if (vectors.size() <= targetSize) {
            float[] centroid = new float[dimension];
            for (int i = 0; i < vectors.size(); i++) {
                float[] vector = vectors.vectorValue(i);
                for (int j = 0; j < dimension; j++) {
                    centroid[j] += vector[j];
                }
            }
            for (int j = 0; j < dimension; j++) {
                centroid[j] /= vectors.size();
            }
            return new KMeansIntermediate(new float[][] { centroid }, new int[vectors.size()]);
        }

        int k = Math.clamp((int) ((vectors.size() + targetSize / 2.0f) / (float) targetSize), 2, MAXK);

        // Also consider prior clustering density: if prior segments had smaller
        // average cluster size, we need more centroids to maintain that granularity
        double meanClusterSize = 0;
        for (int s : clusterSizes) {
            meanClusterSize += s;
        }
        meanClusterSize /= clusterSizes.length;
        if (meanClusterSize > 0 && meanClusterSize < targetSize) {
            int kFromPrior = (int) (vectors.size() / meanClusterSize);
            k = Math.clamp(Math.max(k, kFromPrior), 2, MAXK);
        }

        // Fit prior centroids to target K using variance-aware reduction
        float[][] seedCentroids;
        if (allPriorCentroids.length == k) {
            seedCentroids = deepCopy(allPriorCentroids);
        } else if (allPriorCentroids.length > k) {
            seedCentroids = reduceVarianceAware(allPriorCentroids, clusterSizes, k);
        } else {
            seedCentroids = supplementCentroids(allPriorCentroids, vectors, k);
        }

        // Single assignment pass with NO iterative convergence (0 sampled iterations).
        int[] assignments = new int[vectors.size()];
        Arrays.fill(assignments, -1);
        KMeansIntermediate kMeansIntermediate = new KMeansIntermediate(seedCentroids, assignments, vectors::ordToDoc);
        // maxIterations=0 means only the final full-pass assignment (no sampled iterations)
        KMeansLocal kMeansLocal = buildKmeansLocal(vectors.size(), vectors.size(), 0);
        kMeansLocal.cluster(vectors, kMeansIntermediate);
//=======
    int updateAssignmentsWithRecursiveSplit(KMeansIntermediate current, int cluster, KMeansIntermediate subPartitions) {
        if (subPartitions.centroids().length == 0) {
            return 0; // nothing to do, sub-partitions is empty
        }
        int orgCentroidsSize = current.centroids().length;
        int newCentroidsSize = current.centroids().length + subPartitions.centroids().length - 1;

        // update based on the outcomes from the split clusters recursion
        float[][] newCentroids = new float[newCentroidsSize][];
        int[] newClusterCounts = new int[newCentroidsSize];

        // copy centroids prior to the split
        System.arraycopy(current.centroids(), 0, newCentroids, 0, cluster);
        System.arraycopy(current.clusterCounts(), 0, newClusterCounts, 0, cluster);
        // insert the split partitions replacing the original cluster
        System.arraycopy(subPartitions.centroids(), 0, newCentroids, cluster, subPartitions.centroids().length);
        System.arraycopy(subPartitions.clusterCounts(), 0, newClusterCounts, cluster, subPartitions.centroids().length);
        // append the remainder
        System.arraycopy(
            current.centroids(),
            cluster + 1,
            newCentroids,
            cluster + subPartitions.centroids().length,
            orgCentroidsSize - cluster - 1
        );
        System.arraycopy(
            current.clusterCounts(),
            cluster + 1,
            newClusterCounts,
            cluster + subPartitions.centroids().length,
            orgCentroidsSize - cluster - 1
        );

        assert Arrays.stream(newCentroids).allMatch(Objects::nonNull);
        current.setCentroids(newCentroids, newClusterCounts);
//>>>>>>> main

        // Handle oversized clusters
        int[] centroidVectorCount = new int[seedCentroids.length];
        for (int assignment : assignments) {
            if (assignment >= 0) {
                centroidVectorCount[assignment]++;
            }
        }
        resolveOversizedAndEmptyClusters(kMeansIntermediate, vectors, centroidVectorCount, targetSize);

        // SOAR assignment only — benchmarks showed refinement pass never justifies its cost
        if (kMeansIntermediate.centroids().length > 1 && kMeansIntermediate.centroids().length < vectors.size()) {
            assignSoarOnly(vectors, kMeansIntermediate);
        }

        return kMeansIntermediate;
    }

    private void resolveOversizedAndEmptyClusters(
        KMeansIntermediate kMeansIntermediate,
        ClusteringFloatVectorValues vectors,
        int[] centroidVectorCount,
        int targetSize
    ) throws IOException {
        int[] assignments = kMeansIntermediate.assignments();
        float[][] centroids = kMeansIntermediate.centroids();

        FixedBitSet isSplit = new FixedBitSet(centroids.length);
        boolean hasSplitOrEmpty = false;
        for (int c = 0; c < centroids.length; c++) {
            int count = centroidVectorCount[c];
            if (count == 0) {
                hasSplitOrEmpty = true;
            } else if (count > OVERSIZED_THRESHOLD * targetSize) {
                isSplit.set(c);
                hasSplitOrEmpty = true;
            }
        }

        if (!hasSplitOrEmpty) {
            return;
        }

        int[][] clusterSlices = new int[centroids.length][];
        int[] sliceIndex = new int[centroids.length];
        for (int c = 0; c < centroids.length; c++) {
            if (isSplit.get(c)) {
                clusterSlices[c] = new int[centroidVectorCount[c]];
            }
        }
        for (int i = 0; i < assignments.length; i++) {
            int c = assignments[i];
            if (c >= 0 && isSplit.get(c)) {
                clusterSlices[c][sliceIndex[c]++] = i;
            }
        }

        KMeansIntermediate[] subPartitionsList = new KMeansIntermediate[centroids.length];
        int newCentroidsCount = 0;
        for (int c = 0; c < centroids.length; c++) {
            if (isSplit.get(c)) {
                int finalC = c;
                int[] slice = clusterSlices[c];
                ClusteringFloatVectorValues sample = new ClusteringFloatVectorValuesSlice(vectors, i -> slice[i], centroidVectorCount[c]);
                KMeansIntermediate subPartitions = clusterAndSplit(sample, targetSize);
                subPartitionsList[c] = subPartitions;
                newCentroidsCount += subPartitions.centroids().length;
            } else if (centroidVectorCount[c] > 0) {
                newCentroidsCount++;
            }
        }

        float[][] newCentroids = new float[newCentroidsCount][];
        int[] centroidOffsetMap = new int[centroids.length];
        int currentDest = 0;

        for (int c = 0; c < centroids.length; c++) {
            centroidOffsetMap[c] = currentDest;
            if (isSplit.get(c)) {
                KMeansIntermediate sub = subPartitionsList[c];
                System.arraycopy(sub.centroids(), 0, newCentroids, currentDest, sub.centroids().length);
                currentDest += sub.centroids().length;
            } else if (centroidVectorCount[c] > 0) {
                newCentroids[currentDest] = centroids[c];
                currentDest++;
            }
        }

        for (int i = 0; i < assignments.length; i++) {
            int c = assignments[i];
            if (c >= 0 && isSplit.get(c) == false) {
                assignments[i] = centroidOffsetMap[c];
            }
        }

        for (int c = 0; c < centroids.length; c++) {
            if (isSplit.get(c)) {
                KMeansIntermediate sub = subPartitionsList[c];
                int baseOffset = centroidOffsetMap[c];
                int[] slice = clusterSlices[c];
                int[] subAssignments = sub.assignments();
                for (int j = 0; j < subAssignments.length; j++) {
                    int parentOrd = sub.ordToDoc(j);
                    assignments[parentOrd] = baseOffset + subAssignments[j];
                }
            }
        }

        kMeansIntermediate.setCentroids(newCentroids);
    }
}
