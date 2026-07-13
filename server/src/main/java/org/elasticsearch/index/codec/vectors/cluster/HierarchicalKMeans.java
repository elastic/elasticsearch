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
import org.elasticsearch.core.WelfordVariance;
import org.elasticsearch.index.codec.vectors.diskbbq.SoarAssignments;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;

/**
 * An implementation of the hierarchical k-means algorithm that better partitions data than naive k-means.
 *
 * @param <V> the array type for vectors and centroids ({@code float[]} or {@code byte[]})
 */
public class HierarchicalKMeans<V> {
    private static final Logger logger = LogManager.getLogger(HierarchicalKMeans.class);

    public static final int MAXK = 128;
    public static final int SAMPLES_PER_CLUSTER_DEFAULT = 64;
    public static final float DEFAULT_SOAR_LAMBDA = 1.0f;
    private static final int MIN_VECTORS_PER_THREAD = 64;

    public static final boolean USE_BALANCING = true;
    public static final int MAX_ITERATIONS_DEFAULT = USE_BALANCING ? 2 : 6;

    // Hyperparameter for capacity-constrained assignment in reduceVarianceAware
    // Allowed overflow above ceil(totalVectors / k) per meta-cluster. Tighter values produce
    // more balanced output sizes; looser values preserve geometric locality.
    static final float CAPACITY_SLACK = 0.25f;

    // Concat-path quality gate. When the greedy concat path produces a clustering that's likely
    // degenerate, clusterByConcatenation bails out and re-runs the full hierarchical rebuild.
    // Z_THRESHOLD: max cluster size's z-score above which we flag as a catastrophic outlier.
    // EMPTY_FRACTION: fraction of empty clusters above which we bail (duplicate seeds).
    // PRIOR_ZERO_FRACTION: fraction of prior clusterSizes equal to zero above which we bail pre-emptively.
    static final float Z_THRESHOLD = 3.0f;
    static final float EMPTY_FRACTION = 0.1f;
    static final float PRIOR_ZERO_FRACTION = 0.5f;

    final int dimension;
    final int maxIterations;
    final int samplesPerCluster;
    final int clustersPerNeighborhood;
    final float soarLambda;
    final CentroidOps<V> ops;

    private final TaskExecutor executor;
    private final int numWorkers;

    private HierarchicalKMeans(
        CentroidOps<V> ops,
        int dimension,
        TaskExecutor executor,
        int numWorkers,
        int maxIterations,
        int samplesPerCluster,
        int clustersPerNeighborhood,
        float soarLambda
    ) {
        this.ops = ops;
        this.dimension = dimension;
        this.executor = executor;
        this.numWorkers = numWorkers;
        this.maxIterations = maxIterations;
        this.samplesPerCluster = samplesPerCluster;
        this.clustersPerNeighborhood = clustersPerNeighborhood;
        this.soarLambda = soarLambda;
    }

    public static <V> HierarchicalKMeans<V> ofSerial(CentroidOps<V> ops, int dimension) {
        return ofSerial(ops, dimension, MAX_ITERATIONS_DEFAULT, SAMPLES_PER_CLUSTER_DEFAULT, MAXK, DEFAULT_SOAR_LAMBDA);
    }

    public static <V> HierarchicalKMeans<V> ofSerial(
        CentroidOps<V> ops,
        int dimension,
        int maxIterations,
        int samplesPerCluster,
        int clustersPerNeighborhood,
        float soarLambda
    ) {
        return new HierarchicalKMeans<>(ops, dimension, null, 1, maxIterations, samplesPerCluster, clustersPerNeighborhood, soarLambda);
    }

    public static <V> HierarchicalKMeans<V> ofConcurrent(CentroidOps<V> ops, int dimension, TaskExecutor executor, int numWorkers) {
        return ofConcurrent(
            ops,
            dimension,
            executor,
            numWorkers,
            MAX_ITERATIONS_DEFAULT,
            SAMPLES_PER_CLUSTER_DEFAULT,
            MAXK,
            DEFAULT_SOAR_LAMBDA
        );
    }

    public static <V> HierarchicalKMeans<V> ofConcurrent(
        CentroidOps<V> ops,
        int dimension,
        TaskExecutor executor,
        int numWorkers,
        int maxIterations,
        int samplesPerCluster,
        int clustersPerNeighborhood,
        float soarLambda
    ) {
        return new HierarchicalKMeans<>(
            ops,
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
    public KMeansWithOverspill<V> cluster(ClusteringVectorValues<V> vectors, int targetSize) throws IOException {
        return cluster(vectors, targetSize, null);
    }

    /**
     * Like {@link #cluster(ClusteringVectorValues, int)} but seeds the top-level Lloyd pass with
     * {@code warmStartCentroids}. When the warm-start length equals the computed cluster count, all
     * centroid <em>contents</em> are deep-copied into fresh arrays. When it is smaller, remaining
     * centroids are picked from {@code vectors}; when it is larger, only the first {@code k}
     * centroids are used. The caller's {@code warmStartCentroids} arrays are never mutated.
     */
    public KMeansWithOverspill<V> cluster(ClusteringVectorValues<V> vectors, int targetSize, V[] warmStartCentroids) throws IOException {
        if (vectors.size() == 0) {
            return new KMeansWithOverspill<>(KMeansResult.empty(ops), null);
        }

        // if we have a small number of vectors calculate the centroid directly
        if (vectors.size() <= targetSize) {
            V centroid = ops.computeMeanCentroid(vectors, dimension);
            V[] centroids = ops.newCentroidArrayShallow(1);
            centroids[0] = centroid;
            KMeansResult<V> result = new KMeansResult<>(centroids, new int[vectors.size()]);
            // All vectors are assigned to the single centroid (index 0)
            result.setCentroids(centroids, new int[] { vectors.size() });
            return new KMeansWithOverspill<>(result, null);
        }

        // partition the space
        KMeansResult<V> kMeansResult = clusterAndSplit(vectors, targetSize, warmStartCentroids);

        if (logger.isDebugEnabled()) {
            logger.debug("Hierarchical clustering stats (pre-SOAR):");
            logClusterQualityStatistics(vectors, kMeansResult);
        }

        if (kMeansResult.centroids().length > 1 && kMeansResult.centroids().length < vectors.size()) {
            int localSampleSize = Math.min(kMeansResult.centroids().length * samplesPerCluster / 2, vectors.size());
            KMeansLocal<V> kMeansLocal = buildKmeansLocalFinal(vectors.size(), localSampleSize);
            KMeansWithOverspill<V> res = kMeansLocal.cluster(vectors, kMeansResult, clustersPerNeighborhood);

            if (logger.isDebugEnabled()) {
                logger.debug("Refinement clustering stats (pre-SOAR):");
                logClusterQualityStatistics(vectors, kMeansResult);
            }
            return res;
        }

        return new KMeansWithOverspill<>(kMeansResult, null);
    }

    /**
     * Number of top-level clusters {@link #cluster(ClusteringVectorValues, int)} targets before recursive splitting.
     */
    static int numClustersForTargetSize(int vectorCount, int targetSize) {
        return Math.clamp((int) ((vectorCount + targetSize / 2.0f) / (float) targetSize), 2, MAXK);
    }

    private KMeansIntermediate<V> clusterAndSplit(final ClusteringVectorValues<V> vectors, final int targetSize, V[] warmStartCentroids)
        throws IOException {
        if (vectors.size() <= targetSize) {
            return KMeansIntermediate.empty(ops);
        }

        int k = numClustersForTargetSize(vectors.size(), targetSize);
        int m = Math.min(k * samplesPerCluster, vectors.size());

        // TODO: instead of creating a sub-cluster assignments reuse the parent array each time
        int[] assignments = new int[vectors.size()];
        // ensure we don't over assign to cluster 0 without adjusting it
        Arrays.fill(assignments, -1);
        V[] centroids = initialCentroidsForClustering(vectors, k, warmStartCentroids, ops);
        KMeansIntermediate<V> kMeansIntermediate = new KMeansIntermediate<>(centroids, assignments, vectors::ordToDoc);
        KMeansLocal<V> kMeansLocal = buildKmeansLocal(vectors.size(), m);
        kMeansLocal.cluster(vectors, kMeansIntermediate);

        centroids = kMeansIntermediate.centroids();
        int[] centroidVectorCount = kMeansIntermediate.clusterCounts();

        if (centroids.length == 1) {
            return kMeansIntermediate;
        }

        int centroidIndexOffset = 0; // tracks the cumulative change in centroid indices due to splits and removals
        for (int c = 0; c < centroidVectorCount.length; c++) {
            // Recurse for each cluster which is larger than targetSize
            final int count = centroidVectorCount[c];
            final int adjustedCentroid = c + centroidIndexOffset;
            if (count > targetSize) {
                final ClusteringVectorValues<V> sample = createClusterSlice(count, adjustedCentroid, vectors, assignments);

                // TODO: consider iterative here instead of recursive
                // recursive call to build out the sub partitions around this centroid c
                // subsequently reconcile and flatten the space of all centroids and assignments into one structure we can return
                KMeansIntermediate<V> subPartitions = clusterAndSplit(sample, targetSize, null);
                // Update offset: split replaces 1 centroid with subPartitions.centroids().length centroids
                centroidIndexOffset += updateAssignmentsWithRecursiveSplit(kMeansIntermediate, adjustedCentroid, subPartitions);
            }
        }

        return kMeansIntermediate;
    }

    private static <V> V[] initialCentroidsForClustering(
        ClusteringVectorValues<V> vectors,
        int k,
        V[] warmStartCentroids,
        CentroidOps<V> ops
    ) throws IOException {
        if (warmStartCentroids == null || warmStartCentroids.length == 0) {
            return KMeansLocal.pickInitialCentroids(vectors, k, ops);
        }
        V[] centroids = ops.newCentroidArray(k, vectors.dimension());
        if (warmStartCentroids.length >= k) {
            // deep copy: Lloyd passes mutate centroid arrays in place; the caller must retain
            // ownership of warmStartCentroids for subsequent iterations
            ops.arrayCopyDeep(warmStartCentroids, 0, centroids, 0, k);
            return centroids;
        }
        int warm = warmStartCentroids.length;
        // same ownership reason as above for the warm-start prefix
        ops.arrayCopyDeep(warmStartCentroids, 0, centroids, 0, warm);
        V[] additional = KMeansLocal.pickInitialCentroids(vectors, k - warm, ops);
        // additional inner arrays are freshly allocated by pickInitialCentroids, safe to copy by reference
        ops.arrayCopy(additional, 0, centroids, warm, k - warm);
        return centroids;
    }

    private KMeansLocal<V> buildKmeansLocal(int numVectors, int localSampleSize) {
        return buildKmeansLocal(numVectors, localSampleSize, maxIterations);
    }

    private KMeansLocal<V> buildKmeansLocal(int numVectors, int localSampleSize, int iterations) {
        int numWorkers = Math.min(this.numWorkers, numVectors / MIN_VECTORS_PER_THREAD);
        // if there is no executor or there is no enough vectors for more than one thread, use the serial version
        if (USE_BALANCING) {
            return executor == null || numWorkers <= 1
                ? new BalancedOTKMeansLocalSerial<>(ops, localSampleSize, iterations, soarLambda)
                : new BalancedOTKMeansLocalConcurrent<>(ops, executor, numWorkers, localSampleSize, iterations, soarLambda);
        } else {
            return executor == null || numWorkers <= 1
                ? new LloydKMeansLocalSerial<>(ops, localSampleSize, iterations, soarLambda)
                : new LloydKMeansLocalConcurrent<>(ops, executor, numWorkers, localSampleSize, iterations, soarLambda);
        }
    }

    private KMeansLocal<V> buildKmeansLocalFinal(int numVectors, int localSampleSize) {
        int numWorkers = Math.min(this.numWorkers, numVectors / MIN_VECTORS_PER_THREAD);
        // if there is no executor or there is no enough vectors for more than one thread, use the serial version
        if (USE_BALANCING) {
            return executor == null || numWorkers <= 1
                ? new BalancedASKMeansLocalSerial<>(ops, localSampleSize, maxIterations, soarLambda)
                : new BalancedASKMeansLocalConcurrent<>(ops, executor, numWorkers, localSampleSize, maxIterations, soarLambda);
        } else {
            return executor == null || numWorkers <= 1
                ? new LloydKMeansLocalSerial<>(ops, localSampleSize, maxIterations, soarLambda)
                : new LloydKMeansLocalConcurrent<>(ops, executor, numWorkers, localSampleSize, maxIterations, soarLambda);
        }
    }

    static <V> ClusteringVectorValues<V> createClusterSlice(
        int clusterSize,
        int cluster,
        ClusteringVectorValues<V> vectors,
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

        return new ClusteringVectorValuesSlice<>(vectors, i -> slice[i], slice.length);
    }

    /**
     * Builds a {@link KMeansResult} with a single centroid equal to the mean of all vectors.
     * Used by the small-corpus early-return path in {@link #cluster}, {@link #clusterByInsertion},
     * and {@link #clusterByConcatenation}.
     */
    private KMeansResult<V> singleCentroidResult(ClusteringVectorValues<V> vectors) throws IOException {
        V centroid = ops.computeMeanCentroid(vectors, dimension);
        V[] centroids = ops.newCentroidArrayShallow(1);
        centroids[0] = centroid;
        return new KMeansResult<>(centroids, new int[vectors.size()]);
    }

    /**
     * For each cluster whose size exceeds {@code targetSize}, recursively splits it via
     * {@link #clusterAndSplit} and inserts the resulting sub-centroids in place of the parent
     * (shifting following assignment indices). The {@code centroidIndexOffset} accumulates
     * insertions so each iteration addresses the correct (shifted) centroid slot.
     */
    private void splitOversizedClusters(
        ClusteringVectorValues<V> vectors,
        KMeansResult<V> kMeansResult,
        int[] centroidVectorCount,
        int[] assignments,
        int targetSize
    ) throws IOException {
        int centroidIndexOffset = 0;
        for (int c = 0; c < centroidVectorCount.length; c++) {
            final int count = centroidVectorCount[c];
            final int adjustedCentroid = c + centroidIndexOffset;
            if (count > targetSize) {
                final ClusteringVectorValues<V> sample = createClusterSlice(count, adjustedCentroid, vectors, assignments);
                KMeansIntermediate<V> subPartitions = clusterAndSplit(sample, targetSize, null);
                centroidIndexOffset += updateAssignmentsWithRecursiveSplit(kMeansResult, adjustedCentroid, subPartitions);
            }
        }
    }

    /** Consumer for {@link #forEachSquareDistance}: receives (candidate index, squared distance). */
    @FunctionalInterface
    private interface DistanceVisitor {
        void accept(int index, float squareDistance);
    }

    /**
     * Computes squared distances from {@code query} to {@code candidates[from..to)} using bulk SIMD
     * scoring (4-wide) with a scalar tail, invoking {@code visitor} for each result.
     */
    private void forEachSquareDistance(V query, V[] candidates, int from, int to, DistanceVisitor visitor) {
        final float[] buf = new float[4];
        final int bulkLimit = to - 3;
        int i = from;
        for (; i < bulkLimit; i += 4) {
            ops.squareDistanceBulk(query, candidates[i], candidates[i + 1], candidates[i + 2], candidates[i + 3], 0, buf);
            visitor.accept(i, buf[0]);
            visitor.accept(i + 1, buf[1]);
            visitor.accept(i + 2, buf[2]);
            visitor.accept(i + 3, buf[3]);
        }
        for (; i < to; i++) {
            visitor.accept(i, ops.squareDistance(query, candidates[i]));
        }
    }

    /**
     * Streaming variant of {@link #forEachSquareDistance(Object, Object[], int, int, DistanceVisitor)}: reads four
     * candidates at a time into the caller-supplied {@code window} buffer so the bulk-4 SIMD path
     * still applies even when the candidate vectors are off-heap. The window must be sized
     * {@code V[4]} with each element having length {@code candidates.dimension()}.
     */
    private void forEachSquareDistance(V query, ClusteringVectorValues<V> candidates, int from, int to, V[] window, DistanceVisitor visitor)
        throws IOException {
        assert window.length == 4;
        final float[] buf = new float[4];
        final int bulkLimit = to - 3;
        int i = from;
        for (; i < bulkLimit; i += 4) {
            for (int j = 0; j < 4; j++) {
                V v = candidates.vectorValue(i + j);
                ops.initCentroid(window[j], v, candidates.dimension());
            }
            ops.squareDistanceBulk(query, window[0], window[1], window[2], window[3], 0, buf);
            visitor.accept(i, buf[0]);
            visitor.accept(i + 1, buf[1]);
            visitor.accept(i + 2, buf[2]);
            visitor.accept(i + 3, buf[3]);
        }
        for (; i < to; i++) {
            visitor.accept(i, ops.squareDistance(query, candidates.vectorValue(i)));
        }
    }

    /**
     * Materializes a streaming {@link ClusteringVectorValues} into a {@code V[n]}.
     * Each vector is copied because {@link ClusteringVectorValues#vectorValue(int)} may
     * return a reused scratch buffer. Only safe to call when {@code values.size()} is bounded
     * (e.g. by {@link #MAXK}); large inputs must be streamed.
     */
    private V[] materialize(ClusteringVectorValues<V> values) throws IOException {
        int n = values.size();
        V[] copy = ops.newCentroidArrayShallow(n);
        for (int i = 0; i < n; i++) {
            V v = values.vectorValue(i);
            copy[i] = ops.copyOf(v);
        }
        return copy;
    }

    /**
     * Reduce a large set of centroids to targetCount using lightweight K-means.
     * Operates only on centroids (not full vectors), so it's very fast. The {@code centroids}
     * view is streamed without materializing the full array on the heap.
     */
    private V[] reduceCentroids(ClusteringVectorValues<V> centroids, int targetCount) throws IOException {
        int n = centroids.size();
        V[] reduced = KMeansLocal.pickInitialCentroids(centroids, targetCount, ops);
        // Run a few Lloyd iterations to refine
        KMeansResult<V> result = new KMeansResult<>(reduced, new int[n]);
        KMeansLocal<V> kMeansLocal = new LloydKMeansLocalSerial<>(ops, n, 3, -1);
        kMeansLocal.cluster(centroids, result);
        return result.centroids();
    }

    /**
     * Reduces N prior centroids to targetCount seed centroids for the concatenation merge path,
     * ensuring each meta-cluster represents roughly equal total vector mass.
     * <p>
     * Uses mass-weighted seed selection (proportional to mass × distance²) followed by a
     * single capacity-constrained greedy assignment pass — no iterative solver is run, keeping
     * the merge path fast.
     * <p>
     * Internally accumulates in float precision regardless of vector type (centroid sets are
     * small, so the extra float sums are negligible) and produces native-type results via
     * {@link CentroidOps#computeMeanCentroid}.
     *
     * @param priors       prior centroids from all input segments
     * @param clusterSizes number of vectors per centroid in the prior segments
     * @param targetCount  desired centroid count
     * @return reduced centroids in native type V
     */
    private V[] reduceVarianceAware(ClusteringVectorValues<V> priors, int[] clusterSizes, int targetCount) throws IOException {
        int n = priors.size();
        int k = Math.min(targetCount, n);
        logger.debug("reduceVarianceAware: reducing [{}] centroids to [{}]", n, k);

        // Step 1: Mass-weighted seed selection — each seed is sampled proportional to
        // mass × distance², placing seeds in high-mass regions so meta-clusters start
        // with roughly equal vector mass.
        V[] seeds = massWeightedSeedSelection(priors, clusterSizes, k);

        // Step 2: Capacity-constrained greedy assignment — single pass, no iteration.
        // Assigns centroids to their nearest seed while capping each meta-cluster at
        // ceil(totalVectors/k) inflated by CAPACITY_SLACK, bounding output size variance.
        int[] assignments = capacityConstrainedAssign(priors, clusterSizes, seeds, k);

        // Step 3: Compute size-weighted mean centroid for each meta-cluster.
        // We accumulate in float precision (sums) regardless of V type to avoid overflow
        // for byte vectors. The final result is converted to the native type.
        int[] totalSizes = new int[k];
        float[][] sums = new float[k][dimension];
        int nonZero = 0;
        for (int i = 0; i < n; i++) {
            int c = assignments[i];
            if (clusterSizes[i] > 0 && totalSizes[c] == 0) {
                nonZero++;
            }
            totalSizes[c] += clusterSizes[i];
            V v = priors.vectorValue(i);
            ops.addScaled(clusterSizes[i], v, sums[c]);
        }
        V[] result = ops.newCentroidArrayShallow(nonZero);
        int outIdx = 0;
        for (int c = 0; c < k; c++) {
            if (totalSizes[c] > 0) {
                float invSize = 1.0f / totalSizes[c];
                for (int d = 0; d < dimension; d++) {
                    sums[c][d] *= invSize;
                }
                result[outIdx] = ops.centroidFromFloat(sums[c], dimension);
                outIdx++;
            }
        }
        logger.debug("reduceVarianceAware: output [{}] centroids", result.length);
        return result;
    }

    /**
     * Sequential seed selection weighted by centroid mass (vector count). The first seed is
     * sampled proportional to mass; each subsequent seed is sampled proportional to mass × distance²
     * to the nearest already-selected seed. This places seeds in high-mass regions so that each
     * resulting meta-cluster starts with roughly equal total vector mass.
     */
    private V[] massWeightedSeedSelection(ClusteringVectorValues<V> priors, int[] clusterSizes, int k) throws IOException {
        int n = priors.size();
        k = Math.min(k, n);
        V[] selected = ops.newCentroidArrayShallow(k);
        Random random = new Random(42L);

        // First seed: proportional to cluster size, biasing toward high-density regions.
        long totalSize = 0;
        for (int s : clusterSizes) {
            totalSize += s;
        }
        int firstIdx;
        if (totalSize <= 0) {
            // Degenerate: all prior cluster sizes are zero. Pick uniformly at random.
            firstIdx = random.nextInt(n);
        } else {
            double r = random.nextDouble() * totalSize;
            double cum = 0;
            firstIdx = n - 1;
            for (int i = 0; i < n; i++) {
                cum += clusterSizes[i];
                if (cum > r) {
                    firstIdx = i;
                    break;
                }
            }
        }
        // priors.vectorValue may return a reused scratch buffer; copy the chosen seed.
        selected[0] = ops.copyOf(priors.vectorValue(firstIdx));

        float[] minDist = new float[n];
        Arrays.fill(minDist, Float.MAX_VALUE);

        // Window used by the streaming forEachSquareDistance overload; allocated once.
        V[] window = ops.newCentroidArray(4, dimension);

        // weights[j] = clusterSizes[j] * minDist[j]; cached to avoid recomputing in the selection pass.
        // double (not float) so the cumulative-weight sampling loop below doesn't lose small entries
        // once `cumulative` grows large (squared distances × cluster sizes can easily reach ~1e9).
        double[] weights = new double[n];
        double[] totalWeightHolder = new double[1];
        for (int c = 1; c < k; c++) {
            totalWeightHolder[0] = 0;
            forEachSquareDistance(selected[c - 1], priors, 0, n, window, (idx, d) -> {
                if (d < minDist[idx]) {
                    minDist[idx] = d;
                }
                weights[idx] = (double) clusterSizes[idx] * minDist[idx];
                totalWeightHolder[0] += weights[idx];
            });
            double totalWeight = totalWeightHolder[0];
            int chosen;
            if (totalWeight <= 0) {
                // Degenerate: every remaining candidate has zero weight (sizes all zero, or all
                // already-selected centroids cover the space). Fall back to uniform sampling so
                // we still produce k seeds rather than re-picking centroids[0] deterministically.
                chosen = random.nextInt(n);
            } else {
                // rv requires totalWeight to be fully accumulated before sampling, so two passes are necessary.
                double rv = random.nextDouble() * totalWeight;
                double cumulative = 0;
                chosen = n - 1;
                for (int jj = 0; jj < n; jj++) {
                    cumulative += weights[jj];
                    if (cumulative >= rv) {
                        chosen = jj;
                        break;
                    }
                }
            }
            selected[c] = ops.copyOf(priors.vectorValue(chosen));
        }
        return selected;
    }

    /**
     * Assigns each centroid to its nearest seed subject to a per-seed vector-count capacity of
     * ceil(totalVectors/k) inflated by {@link #CAPACITY_SLACK}. Centroids are processed in
     * descending order of assignment regret (gap between nearest and second-nearest seed distance)
     * so the most constrained assignments are resolved first. Falls back to the geometrically
     * nearest seed if all seeds are at capacity.
     */
    private int[] capacityConstrainedAssign(ClusteringVectorValues<V> priors, int[] clusterSizes, V[] seeds, int k) throws IOException {
        int n = priors.size();

        long totalVectors = 0;
        for (int s : clusterSizes) {
            totalVectors += s;
        }
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
            final int row = i;
            // seeds is small (k ≤ MAXK) and stays on heap; the existing bulk-4 SIMD path applies.
            // priors.vectorValue(i) may return a reused scratch buffer — safe here because
            // forEachSquareDistance does not fetch any more vectors from priors during the call.
            forEachSquareDistance(priors.vectorValue(i), seeds, 0, k, (c, d) -> {
                dist[row][c] = d;
                if (d < nearestDist[row]) {
                    secondDist[row] = nearestDist[row];
                    nearestDist[row] = d;
                    nearest[row] = c;
                } else if (d < secondDist[row]) {
                    secondDist[row] = d;
                }
            });
        }

        // Sort by descending regret so the centroid with the highest cost of displacement
        // is assigned to its preferred seed before that seed can fill up.
        // Inline IntroSorter over a primitive int[] avoids the Integer[] boxing + Comparator allocation.
        int[] order = new int[n];
        for (int i = 0; i < n; i++) {
            order[i] = i;
        }
        new org.apache.lucene.util.IntroSorter() {
            float pivot;

            @Override
            protected void setPivot(int i) {
                int o = order[i];
                pivot = secondDist[o] - nearestDist[o];
            }

            @Override
            protected int comparePivot(int j) {
                int o = order[j];
                // descending: larger regret sorts first
                return Float.compare(secondDist[o] - nearestDist[o], pivot);
            }

            @Override
            protected void swap(int i, int j) {
                int tmp = order[i];
                order[i] = order[j];
                order[j] = tmp;
            }
        }.sort(0, n);

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

    private SoarAssignments assignSoarOnly(ClusteringVectorValues<V> vectors, KMeansResult<V> kMeansResult) throws IOException {
        if (soarLambda < 0) {
            return null;
        }

        NeighborHood[] neighborhoods = null;
        V[] centroids = kMeansResult.centroids();
        if (centroids.length > clustersPerNeighborhood) {
            neighborhoods = executor == null || numWorkers < 2
                ? NeighborHood.computeNeighborhoods(ops, centroids, clustersPerNeighborhood)
                : NeighborHood.computeNeighborhoods(ops, executor, numWorkers, centroids, clustersPerNeighborhood);
        }

        int effectiveWorkers = Math.min(numWorkers, vectors.size() / MIN_VECTORS_PER_THREAD);
        if (executor != null && effectiveWorkers >= 2) {
            return new SoarAssignments(
                Soar.assignSpilledConcurrent(executor, effectiveWorkers, vectors, ops, kMeansResult, neighborhoods, soarLambda)
            );
        } else {
            return new SoarAssignments(Soar.assignSpilledSlice(vectors, ops, kMeansResult, neighborhoods, soarLambda));
        }
    }

    /**
     * Supplement a small set of centroids with greedily-selected vectors to reach targetCount.
     * Uses distance-based sampling on a reservoir sample of the corpus: each iteration picks a
     * new centroid with probability proportional to (clusterSize × distance²) when prior cluster
     * sizes are known, otherwise just distance². The size weighting biases new centroids toward
     * vectors whose nearest existing centroid has a large prior cluster — splitting big clusters
     * to improve the final size balance. Once a vector's nearest centroid becomes a newly-added
     * one, its weight reduces to distance² only (distance-proportional for already-addressed regions).
     */
    private V[] supplementCentroids(
        ClusteringVectorValues<V> existing,
        int[] clusterSizes,
        ClusteringVectorValues<V> vectors,
        int targetCount
    ) throws IOException {
        // existing.size() is bounded by MAXK at the call sites — safe to materialize.
        V[] existingHeap = materialize(existing);
        V[] augmented = ops.newCentroidArrayShallow(targetCount);
        for (int i = 0; i < existingHeap.length; i++) {
            augmented[i] = ops.copyOf(existingHeap[i]);
        }
        int needed = targetCount - existingHeap.length;
        if (needed <= 0 || vectors.size() == 0) {
            return augmented;
        }

        Random random = new Random(42L);
        int n = vectors.size();
        // select a minimum of 4096 vectors (n may be smaller than 4096)
        int sampleSize = Math.clamp(needed * 64L, Math.min(4096, n), n);

        // Reservoir-sample sampleSize vector indices, deterministic with seed 42.
        int[] sample = new int[sampleSize];
        for (int i = 0; i < sampleSize; i++) {
            sample[i] = i;
        }
        for (int i = sampleSize; i < n; i++) {
            int j = random.nextInt(i + 1);
            if (j < sampleSize) sample[j] = i;
        }

        // Materialize sampled vectors once: ClusteringVectorValues#vectorValue may return a
        // reused/decoded-on-demand buffer, so we copy here to (a) avoid re-decoding on every
        // update iteration and (b) allow bulk-4 SIMD scoring across the sample.
        V[] sampleVecs = ops.newCentroidArrayShallow(sampleSize);
        for (int s = 0; s < sampleSize; s++) {
            V v = vectors.vectorValue(sample[s]);
            sampleVecs[s] = ops.copyOf(v);
        }

        // For each sampled vector, find its nearest existing centroid; record min squared dist
        // and which existing centroid it falls under (or -1 once it lands under a newly-added one).
        float[] minDist = new float[sampleSize];
        int[] nearestId = new int[sampleSize];
        Arrays.fill(minDist, Float.MAX_VALUE);
        for (int s = 0; s < sampleSize; s++) {
            final int sIdx = s;
            forEachSquareDistance(sampleVecs[s], existingHeap, 0, existingHeap.length, (c, d) -> {
                if (d < minDist[sIdx]) {
                    minDist[sIdx] = d;
                    nearestId[sIdx] = c;
                }
            });
        }

        for (int it = 0; it < needed; it++) {
            // Build cumulative weight (size × distance² for vectors still nearest to an existing
            // centroid; distance² alone for vectors now nearest to a newly-added one).
            double total = 0;
            for (int s = 0; s < sampleSize; s++) {
                int nid = nearestId[s];
                double w = (clusterSizes != null && nid >= 0 && nid < existingHeap.length)
                    ? (double) clusterSizes[nid] * minDist[s]
                    : minDist[s];
                total += w;
            }

            int chosen;
            if (total <= 0) {
                // Degenerate: all candidates collapsed onto current centroids or all weights are
                // zero (e.g. clusterSizes contains zeros for an empty prior cluster, or the prior
                // covers the data perfectly). Fall back to uniform sampling so every slot still
                // gets a centroid.
                chosen = random.nextInt(sampleSize);
            } else {
                double r = random.nextDouble() * total;
                double cum = 0;
                chosen = sampleSize - 1;
                for (int s = 0; s < sampleSize; s++) {
                    int nid = nearestId[s];
                    double w = (clusterSizes != null && nid >= 0 && nid < existingHeap.length)
                        ? (double) clusterSizes[nid] * minDist[s]
                        : minDist[s];
                    cum += w;
                    if (cum >= r) {
                        chosen = s;
                        break;
                    }
                }
            }

            V vec = sampleVecs[chosen];
            int newIdx = existingHeap.length + it;
            augmented[newIdx] = ops.copyOf(vec);

            // Update minDist / nearestId for the new centroid using bulk SIMD scoring across
            // the materialized sample (query = new centroid, candidates = sample vectors).
            final int target = newIdx;
            forEachSquareDistance(augmented[newIdx], sampleVecs, 0, sampleSize, (s, d) -> {
                if (d < minDist[s]) {
                    minDist[s] = d;
                    nearestId[s] = target;
                }
            });
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
    public KMeansWithOverspill<V> clusterByInsertion(
        ClusteringVectorValues<V> vectors,
        ClusteringVectorValues<V> initialCentroids,
        int targetSize
    ) throws IOException {
        if (vectors.size() == 0) {
            return new KMeansWithOverspill<>(KMeansResult.empty(ops), null);
        }

        if (vectors.size() <= targetSize) {
            return new KMeansWithOverspill<>(singleCentroidResult(vectors), null);
        }

        int k = Math.clamp((int) ((vectors.size() + targetSize / 2.0f) / (float) targetSize), 2, MAXK);

        V[] seedCentroids;
        int priorCount = initialCentroids.size();
        if (priorCount == k) {
            seedCentroids = materialize(initialCentroids);
        } else if (priorCount > k) {
            seedCentroids = reduceCentroids(initialCentroids, k);
        } else {
            seedCentroids = supplementCentroids(initialCentroids, null, vectors, k);
        }

        // Single assignment pass: assign all vectors to nearest seed centroid and update positions.
        // No sampling — with only 1 iteration, a full pass is both faster and more accurate than
        // a sampled pass followed by a final full pass.
        int[] assignments = new int[vectors.size()];
        Arrays.fill(assignments, -1);
        KMeansResult<V> kMeansResult = new KMeansResult<>(seedCentroids, assignments);
        KMeansLocal<V> kMeansLocal = buildKmeansLocal(vectors.size(), vectors.size(), 1);
        kMeansLocal.cluster(vectors, kMeansResult);

        // Handle oversized clusters by recursive splitting, same as clusterAndSplit
        int[] centroidVectorCount = new int[seedCentroids.length];
        for (int assignment : assignments) {
            if (assignment >= 0) {
                centroidVectorCount[assignment]++;
            }
        }

        // Post-check: bail to full rebuild if the single assignment pass produced a degenerate
        // distribution. Mirrors the guard in clusterByConcatenation.
        if (concatClusteringIsDegenerate(centroidVectorCount)) {
            logger.debug("clusterByInsertion: post-assignment distribution is degenerate, falling back to full rebuild");
            return cluster(vectors, targetSize);
        }

        splitOversizedClusters(vectors, kMeansResult, centroidVectorCount, assignments, targetSize);

        // Neighborhood-aware refinement + SOAR with minimal iteration.
        // One pass is sufficient since the initial assignment is already good.
        if (kMeansResult.centroids().length > 1 && kMeansResult.centroids().length < vectors.size()) {
            KMeansLocal<V> refinementKMeans = buildKmeansLocal(vectors.size(), vectors.size(), 1);
            return refinementKMeans.cluster(vectors, kMeansResult, clustersPerNeighborhood);
        }

        return new KMeansWithOverspill<>(kMeansResult, null);
    }

    /**
     * Cluster vectors by concatenating prior centroids and doing a single assignment pass.
     * <p>
     * For merges of balanced segments (no dominant segment) where prior centroids exist:
     * concatenate all prior centroids, reduce/expand to target K, then do a single full
     * assignment pass + SOAR. This eliminates iterative convergence entirely — the prior
     * centroids are already good approximations of the data distribution.
     * <p>
     * Reduction uses mass-weighted seed selection plus a capacity-constrained greedy
     * assignment (see {@link #reduceVarianceAware}), bounding meta-cluster size variance
     * without running a full balancing solver.
     *
     * @param vectors            the merged vectors
     * @param allPriorCentroids  centroids concatenated from all input segments that provided priors
     * @param clusterSizes       number of vectors per centroid in the prior segments
     * @param coveredVectorCount sum of vector counts from segments that contributed priors. Equal to
     *                           {@code vectors.size()} when every input segment exposed priors; smaller
     *                           when some segments did not (e.g. legacy formats whose reader returns
     *                           {@code null} from {@code readCentroidData}). Used to extrapolate the
     *                           prior's density to the uncovered remainder without inflating {@code k}.
     * @param targetSize         target vectors per cluster
     * @return clustering result
     */
    public KMeansWithOverspill<V> clusterByConcatenation(
        ClusteringVectorValues<V> vectors,
        ClusteringVectorValues<V> allPriorCentroids,
        int[] clusterSizes,
        int coveredVectorCount,
        int targetSize
    ) throws IOException {
        if (vectors.size() == 0) {
            return new KMeansWithOverspill<>(KMeansResult.empty(ops), null);
        }
        if (vectors.size() <= targetSize) {
            return new KMeansWithOverspill<>(singleCentroidResult(vectors), null);
        }

        int k = Math.clamp((int) ((vectors.size() + targetSize / 2.0f) / (float) targetSize), 2, MAXK);

        // Also consider prior clustering density: if prior segments had smaller
        // average cluster size, we need more centroids to maintain that granularity.
        // The mean is computed over the COVERED portion only, so the extrapolation to a
        // kFromPrior must scale by coverage rather than dividing the total vector count by
        // a covered-only density (which over-inflates k in mixed-coverage merges, e.g. when
        // ES940/ES920 segments merge with next-gen ones).
        double meanClusterSize = 0;
        for (int s : clusterSizes) {
            meanClusterSize += s;
        }
        if (clusterSizes.length > 0) {
            meanClusterSize /= clusterSizes.length;
        }
        int effectiveCovered = coveredVectorCount > 0 ? coveredVectorCount : vectors.size();
        if (meanClusterSize > 0 && meanClusterSize < targetSize) {
            // Extrapolate covered density to the full corpus: priors imply priorCount centroids
            // for the covered subset, so the full corpus needs priorCount * (total / covered).
            int kFromPrior = (int) Math.round((double) clusterSizes.length * vectors.size() / effectiveCovered);
            k = Math.clamp(Math.max(k, kFromPrior), 2, MAXK);
        }

        int priorCount = allPriorCentroids.size();

        // Pre-check: the prior is too degenerate to give us useful seeds. Bail to full rebuild
        // before we waste compute on a clustering we'd just throw away. The threshold is scaled
        // by coverage so a prior that adequately covers its own segments isn't penalised just
        // because other segments lacked priors.
        int coverageScaledK = (int) Math.ceil((double) k * effectiveCovered / vectors.size());
        if (priorIsDegenerate(priorCount, clusterSizes, coverageScaledK)) {
            logger.debug("clusterByConcatenation: prior is degenerate, falling back to full rebuild");
            return cluster(vectors, targetSize);
        }

        // Fit prior centroids to target K using variance-aware reduction
        V[] seedCentroids;
        if (priorCount == k) {
            seedCentroids = materialize(allPriorCentroids);
        } else if (priorCount > k) {
            seedCentroids = reduceVarianceAware(allPriorCentroids, clusterSizes, k);
        } else {
            seedCentroids = supplementCentroids(allPriorCentroids, clusterSizes, vectors, k);
        }

        // Single assignment pass with NO iterative convergence (0 sampled iterations).
        int[] assignments = new int[vectors.size()];
        Arrays.fill(assignments, -1);
        KMeansResult<V> kMeansResult = new KMeansResult<>(seedCentroids, assignments);
        // maxIterations=0 means only the final full-pass assignment (no sampled iterations)
        KMeansLocal<V> kMeansLocal = buildKmeansLocal(vectors.size(), vectors.size(), 0);
        kMeansLocal.cluster(vectors, kMeansResult);

        // Handle oversized clusters
        int[] centroidVectorCount = new int[seedCentroids.length];
        for (int assignment : assignments) {
            if (assignment >= 0) {
                centroidVectorCount[assignment]++;
            }
        }

        // Post-check: the concat clustering produced a catastrophic distribution (many empty
        // clusters from duplicate seeds, OR one cluster blew up far beyond the rest). Bail and
        // run the full rebuild so the merge isn't ruined.
        if (concatClusteringIsDegenerate(centroidVectorCount)) {
            logger.debug("clusterByConcatenation: post-assignment distribution is degenerate, falling back to full rebuild");
            return cluster(vectors, targetSize);
        }

        splitOversizedClusters(vectors, kMeansResult, centroidVectorCount, assignments, targetSize);

        // SOAR assignment only — benchmarks showed refinement pass never justifies its cost
        if (kMeansResult.centroids().length > 1 && kMeansResult.centroids().length < vectors.size()) {
            return new KMeansWithOverspill<>(kMeansResult, assignSoarOnly(vectors, kMeansResult));
        }

        return new KMeansWithOverspill<>(kMeansResult, null);
    }

    /**
     * Pre-check on the input to the concat path. Returns true if the prior is so degenerate
     * (mostly empty clusters, or far too few centroids vs target k) that we should skip the
     * greedy path entirely and go to the full hierarchical rebuild.
     */
    private static boolean priorIsDegenerate(int priorCount, int[] clusterSizes, int k) {
        if (priorCount == 0 || clusterSizes == null || clusterSizes.length == 0 || priorCount < Math.max(2, k / 4)) {
            return true;
        }
        long totalSize = 0;
        int zeros = 0;
        for (int s : clusterSizes) {
            totalSize += s;
            if (s == 0) {
                zeros++;
            }
        }
        return totalSize == 0 || (double) zeros / clusterSizes.length > PRIOR_ZERO_FRACTION;
    }

    /**
     * Post-check on the concat clustering. Returns true if the per-cluster vector counts indicate
     * a catastrophic outcome — either too many empty clusters (duplicate seeds collapsed onto
     * each other) or a single cluster blew up to a z-score above {@link #Z_THRESHOLD} above the
     * mean (greedy assignment imbalanced the partition).
     */
    private static boolean concatClusteringIsDegenerate(int[] centroidVectorCount) {
        int k = centroidVectorCount.length;
        if (k == 0) {
            return true;
        }

        int empty = 0;
        long sum = 0;
        int max = 0;
        for (int v : centroidVectorCount) {
            if (v == 0) {
                empty++;
            }
            sum += v;
            if (v > max) {
                max = v;
            }
        }
        if (sum == 0 || (double) empty / k > EMPTY_FRACTION) {
            return true;
        }

        double mean = (double) sum / k;
        double sqSum = 0;
        for (int v : centroidVectorCount) {
            double d = v - mean;
            sqSum += d * d;
        }
        double variance = k > 1 ? sqSum / (k - 1) : 0;
        double stddev = Math.sqrt(variance);
        return stddev > 0 && (max - mean) / stddev > Z_THRESHOLD;
    }

    /**
     * Merge the child centroids in {@code subPartitions} into the K-means results.
     * {@code subPartitions} are inserted into the results next to the parent centroid
     * at position {@code cluster}.
     *
     * @param current Current results
     * @param cluster Index of the split centroid
     * @param subPartitions The new centroids resulting from the split
     * @return The number of centroids added excluding the one that is replaced
     */
    int updateAssignmentsWithRecursiveSplit(KMeansResult<V> current, int cluster, KMeansIntermediate<V> subPartitions) {
        if (subPartitions.centroids().length == 0) {
            return 0; // nothing to do, sub-partitions is empty
        }
        int orgCentroidsSize = current.centroids().length;
        int newCentroidsSize = current.centroids().length + subPartitions.centroids().length - 1;

        // update based on the outcomes from the split clusters recursion
        V[] newCentroids = ops.newCentroidArrayShallow(newCentroidsSize);
        int[] newClusterCounts = new int[newCentroidsSize];

        // copy centroids prior to the split
        ops.arrayCopy(current.centroids(), 0, newCentroids, 0, cluster);
        System.arraycopy(current.clusterCounts(), 0, newClusterCounts, 0, cluster);
        // insert the split partitions replacing the original cluster
        ops.arrayCopy(subPartitions.centroids(), 0, newCentroids, cluster, subPartitions.centroids().length);
        System.arraycopy(subPartitions.clusterCounts(), 0, newClusterCounts, cluster, subPartitions.centroids().length);
        // append the remainder
        ops.arrayCopy(
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

        // Reindex remaining (non-split) assignments to account for the inserted centroids.
        // IMPORTANT: do this BEFORE remapping the split-cluster's assignments to avoid double-shifting.
        for (int i = 0; i < current.assignments().length; i++) {
            if (current.assignments()[i] > cluster) {
                current.assignments()[i] = current.assignments()[i] - 1 + subPartitions.centroids().length;
            }
        }
        // Remap each split-cluster vector's assignment to its sub-centroid's new global index.
        for (int i = 0; i < subPartitions.assignments().length; i++) {
            int parentOrd = subPartitions.ordToDoc(i);
            assert current.assignments()[parentOrd] == cluster;
            current.assignments()[parentOrd] = cluster + subPartitions.assignments()[i];
        }

        // number of items inserted with 1 element replaced
        return subPartitions.centroids().length - 1;
    }

    private void logClusterQualityStatistics(ClusteringVectorValues<V> vectors, KMeansResult<V> kMeansResult) throws IOException {
        // We assume that kMeansIntermediate.centroids().length > 0.
        float inertia = kMeansMeanInertia(vectors, kMeansResult);
        int[] clusterSizes = kMeansResult.clusterCounts();

        int minClusterSize = Integer.MAX_VALUE;
        int maxClusterSize = Integer.MIN_VALUE;

        // Use Welford's algorithm to compute the variance in one pass and compute min/max in the same loop
        WelfordVariance clusterSizeStats = new WelfordVariance();
        for (int x : clusterSizes) {
            clusterSizeStats.add(x);
            minClusterSize = Math.min(minClusterSize, x);
            maxClusterSize = Math.max(maxClusterSize, x);
        }

        double stdClusterSizes = clusterSizeStats.populationStdDev();

        logger.debug(
            "Inertia: {}; Centroid count: {} min: {} max: {} mean: {} stdDev: {}",
            inertia,
            clusterSizes.length,
            minClusterSize,
            maxClusterSize,
            clusterSizeStats.mean(),
            stdClusterSizes
        );
    }

    private float kMeansMeanInertia(ClusteringVectorValues<V> vectors, KMeansResult<V> kMeansResult) throws IOException {
        int[] assignments = kMeansResult.assignments();
        V[] centroids = kMeansResult.centroids();

        float mse = 0;
        for (int i = 0; i < vectors.size(); i++) {
            V vec = vectors.vectorValue(i);
            V cent = centroids[assignments[i]];
            float dist = this.ops.squareDistance(vec, cent);
            mse += dist / vectors.size();
        }
        return mse;
    }
}
