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
import org.apache.lucene.util.hnsw.IntToIntFunction;
import org.elasticsearch.index.codec.vectors.diskbbq.SoarAssignments;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Holds serial and concurrent implementations of the SOAR overspill algorithm
 */
public abstract class Soar<V> {

    // the minimum distance that is considered to be "far enough" to a centroid in order to compute the soar distance.
    private static final float SOAR_MIN_DISTANCE = 1e-16f;
    public static final int NO_SOAR_ASSIGNMENT = -1;

    @SuppressWarnings("rawtypes")
    private static final Soar NONE = new Soar<>(null, 0) {
        @Override
        protected SoarAssignments assignSpilled(ClusteringVectorValues vectors, KMeansResult kmeansResult, NeighborHood[] neighborhoods) {
            return null;
        }
    };

    @SuppressWarnings("unchecked")
    static <V> Soar<V> none() {
        return NONE;
    }

    static <V> Soar<V> ofSerial(CentroidOps<V> ops, float soarLambda) {
        return new Serial<>(ops, soarLambda);
    }

    static <V> Soar<V> ofConcurrent(TaskExecutor executor, int numWorkers, CentroidOps<V> ops, float soarLambda) {
        return new Concurrent<>(executor, numWorkers, ops, soarLambda);
    }

    protected final CentroidOps<V> ops;
    protected final float soarLambda;

    private Soar(CentroidOps<V> ops, float soarLambda) {
        if (soarLambda < 0) throw new IllegalArgumentException("Soar should not be used with lambda < 0");
        this.ops = ops;
        this.soarLambda = soarLambda;
    }

    /** assign to each vector the soar assignment */
    protected abstract SoarAssignments assignSpilled(
        ClusteringVectorValues<V> vectors,
        KMeansResult<V> kmeansResult,
        NeighborHood[] neighborhoods
    ) throws IOException;

    private static class Serial<V> extends Soar<V> {

        Serial(CentroidOps<V> ops, float soarLambda) {
            super(ops, soarLambda);
        }

        @Override
        protected SoarAssignments assignSpilled(
            ClusteringVectorValues<V> vectors,
            KMeansResult<V> kmeansResult,
            NeighborHood[] neighborhoods
        ) throws IOException {
            return new SoarAssignments(assignSpilledSlice(vectors, ops, kmeansResult, neighborhoods, soarLambda));
        }
    }

    private static class Concurrent<V> extends Soar<V> {

        private final TaskExecutor executor;
        private final int numWorkers;

        private Concurrent(TaskExecutor executor, int numWorkers, CentroidOps<V> ops, float soarLambda) {
            super(ops, soarLambda);
            this.executor = executor;
            this.numWorkers = numWorkers;
        }

        @Override
        protected SoarAssignments assignSpilled(
            ClusteringVectorValues<V> vectors,
            KMeansResult<V> kmeansResult,
            NeighborHood[] neighborhoods
        ) throws IOException {
            return new SoarAssignments(
                assignSpilledConcurrent(executor, numWorkers, vectors, ops, kmeansResult, neighborhoods, soarLambda)
            );
        }
    }

    protected static <V> int[] assignSpilledSlice(
        ClusteringVectorValues<V> vectors,
        CentroidOps<V> ops,
        KMeansResult<V> kmeans,
        NeighborHood[] neighborhoods,
        float soarLambda
    ) throws IOException {
        int[] assignments = new int[vectors.size()];
        assignSpilledSlice(vectors, ops, kmeans, neighborhoods, soarLambda, 0, vectors.size(), assignments);
        return assignments;
    }

    protected static <V> int[] assignSpilledConcurrent(
        TaskExecutor executor,
        int numWorkers,
        ClusteringVectorValues<V> vectors,
        CentroidOps<V> ops,
        KMeansResult<V> kMeansResult,
        NeighborHood[] neighborhoods,
        float soarLambda
    ) throws IOException {
        int[] assignments = new int[vectors.size()];
        final int len = vectors.size() / numWorkers;
        final List<Callable<Void>> runners = new ArrayList<>(numWorkers);
        for (int i = 0; i < numWorkers; i++) {
            final int start = i * len;
            final int end = i == numWorkers - 1 ? vectors.size() : (i + 1) * len;
            runners.add(() -> {
                assignSpilledSlice(vectors.copy(), ops, kMeansResult, neighborhoods, soarLambda, start, end, assignments);
                return null;
            });
        }
        executor.invokeAll(runners);
        return assignments;
    }

    /** Assign vectors from {@code startOrd} to {@code endOrd} to the SOAR centroid. */
    private static <V> void assignSpilledSlice(
        ClusteringVectorValues<V> vectors,
        CentroidOps<V> ops,
        KMeansResult<V> kmeans,
        NeighborHood[] neighborhoods,
        float soarLambda,
        int startOrd,
        int endOrd,
        int[] spilledAssignments
    ) throws IOException {
        int[] assignments = kmeans.assignments();
        assert assignments != null;
        assert assignments.length == vectors.size();
        assert spilledAssignments.length == vectors.size();
        V[] centroids = kmeans.centroids();
        assignSpilled(vectors, ops, startOrd, endOrd, centroids, neighborhoods, soarLambda, assignments, spilledAssignments);
    }

    /**
     * Assign a secondary ("spilled") centroid to each vector using the SOAR adjusted distance.
     */
    static <V> void assignSpilled(
        ClusteringVectorValues<V> vectors,
        CentroidOps<V> ops,
        int startOrd,
        int endOrd,
        V[] centroids,
        NeighborHood[] neighborhoods,
        float soarLambda,
        int[] assignments,
        int[] spilledAssignments
    ) throws IOException {
        float[] diffs = new float[vectors.dimension()];
        final float[] distances = new float[4];
        for (int i = startOrd; i < endOrd; i++) {
            V vector = vectors.vectorValue(i);
            final int currAssignment = assignments[i];
            final int centroidCount;
            final IntToIntFunction centroidOrds;
            if (neighborhoods != null) {
                assert neighborhoods[currAssignment] != null;
                NeighborHood neighborhood = neighborhoods[currAssignment];
                centroidCount = neighborhood.neighbors().length;
                centroidOrds = c -> neighborhood.neighbors()[c];
            } else {
                centroidCount = centroids.length - 1;
                centroidOrds = c -> c < currAssignment ? c : c + 1; // skip the current centroid
            }
            spilledAssignments[i] = computeSoarAssignment(
                vector,
                centroids,
                currAssignment,
                centroidCount,
                centroidOrds,
                soarLambda,
                diffs,
                distances,
                ops
            );
        }
    }

    private static <V> int computeSoarAssignment(
        V vector,
        V[] centroids,
        int currAssignment,
        int centroidCount,
        IntToIntFunction centroidOrds,
        float soarLambda,
        float[] diffs,
        float[] distances,
        CentroidOps<V> ops
    ) {
        V currentCentroid = centroids[currAssignment];
        float vectorCentroidDist = ops.squareDistance(vector, currentCentroid);
        if (vectorCentroidDist <= SOAR_MIN_DISTANCE) {
            return NO_SOAR_ASSIGNMENT;
        }

        ops.computeDiffs(vector, currentCentroid, diffs);

        final int limit = centroidCount - 3;
        int bestAssignment = -1;
        float minSoar = Float.MAX_VALUE;
        int j = 0;
        for (; j < limit; j += 4) {
            ops.soarDistanceBulk(
                vector,
                centroids[centroidOrds.apply(j)],
                centroids[centroidOrds.apply(j + 1)],
                centroids[centroidOrds.apply(j + 2)],
                centroids[centroidOrds.apply(j + 3)],
                diffs,
                soarLambda,
                vectorCentroidDist,
                distances
            );
            for (int k = 0; k < distances.length; k++) {
                float soar = distances[k];
                if (soar < minSoar) {
                    minSoar = soar;
                    bestAssignment = centroidOrds.apply(j + k);
                }
            }
        }

        for (; j < centroidCount; j++) {
            int centroidOrd = centroidOrds.apply(j);
            float soar = ops.soarDistance(vector, centroids[centroidOrd], diffs, soarLambda, vectorCentroidDist);
            if (soar < minSoar) {
                minSoar = soar;
                bestAssignment = centroidOrd;
            }
        }
        assert bestAssignment != -1 : "Failed to assign soar vector to centroid";
        return bestAssignment;
    }
}
