/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;
import org.elasticsearch.xpack.querysampling.dedup.Stratum;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Balances picks over the dense and sparse parts of the query space: query vectors are grouped in clusters (k-means
 * with k-means++ seeding on the first distinct queries, since online centroids from the first queries collapse on
 * embeddings), and a cluster with fewer distinct queries than average is picked more by {@code (average / count)^β}.
 * The factor is computed at arrival and recorded in the draw probability, so estimates stay unbiased. Use β mildly:
 * unequal weights cost precision. Queries arriving before the fit have no cluster.
 * <p>
 * The clusters are fitted once per node: afterwards each query moves its centroid by a running mean and the counts do not
 * decay, so after a shift in the queries the balance weakens until the node restarts. The estimates stay unbiased.
 * <p>
 * One instance lives as long as the node and keeps the clusters of a space (a field and its dimensions) for each one it
 * meets, at most {@link #MAX_SPACES}: the fields that are searched are not known in advance, and queries of a space
 * beyond the limit are not grouped, which gives them a factor of 1.
 * <p>
 * Used by the pipeline thread only, which is why the spaces need no lock; only the balance, set by a setting, is volatile.
 */
public final class SpatialStrata {

    /** Caps the boost so a barely known cluster does not make every query in it a sure pick. */
    static final double MAX_FACTOR = 10.0;

    static final int MAX_SPACES = 100;

    static final int MIN_WARMUP = 200;

    static final int FIT_ROUNDS = 5;

    private final int clusters;
    private final int warmup;
    private final Supplier<Random> random;
    private final Map<String, Space> spaces = new HashMap<>();
    private volatile double balance;

    /**
     * @param clusters clusters per space, 0 to not group the queries
     */
    public SpatialStrata(int clusters) {
        this(clusters, Math.max(MIN_WARMUP, 10 * clusters), Randomness::get);
    }

    public SpatialStrata(int clusters, int warmup, Supplier<Random> random) {
        assert warmup >= clusters : "there have to be as many queries as clusters to make them from";
        this.clusters = clusters;
        this.warmup = warmup;
        this.random = random;
    }

    public void watch(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(QuerySamplingSettings.SPATIAL_BALANCE, value -> this.balance = value);
    }

    /**
     * The receiver is called on this thread with the cluster, once the clusters are fitted. It is never called if no
     * clusters are wanted, the vector is not finite, or there are too many spaces.
     */
    public void assign(String field, float[] vector, Consumer<Stratum> receiver) {
        if (clusters == 0 || vector.length == 0) {
            return;
        }
        for (float value : vector) {
            if (Float.isFinite(value) == false) {
                return; // it would take the centroid of the cluster that it joins with it
            }
        }
        String name = field + "/" + vector.length;
        Space space = spaces.get(name);
        if (space == null) {
            if (spaces.size() >= MAX_SPACES) {
                return;
            }
            space = new Space(clusters, vector.length, warmup);
            spaces.put(name, space);
        }
        space.add(name, vector, receiver, random);
    }

    public double factor(@Nullable Stratum stratum) {
        double balance = this.balance;
        if (stratum == null || balance == 0.0) {
            return 1.0;
        }
        Space space = spaces.get(stratum.space());
        if (space == null) {
            return 1.0;
        }
        return Math.min(MAX_FACTOR, Math.pow(space.averageCount() / space.counts[stratum.cluster()], balance));
    }

    private static final class Space {
        private final float[][] centroids;
        private final long[] counts;
        private final int warmup;
        private final List<float[]> waitingVectors = new ArrayList<>();
        private final List<Consumer<Stratum>> waitingReceivers = new ArrayList<>();
        private boolean fitted;
        private long total;

        Space(int clusters, int dims, int warmup) {
            this.centroids = new float[clusters][dims];
            this.counts = new long[clusters];
            this.warmup = warmup;
        }

        void add(String name, float[] vector, Consumer<Stratum> receiver, Supplier<Random> random) {
            if (fitted) {
                receiver.accept(new Stratum(name, join(vector)));
                return;
            }
            waitingVectors.add(vector);
            waitingReceivers.add(receiver);
            if (waitingVectors.size() < warmup) {
                return;
            }
            fit(random.get());
            fitted = true;
            for (int i = 0; i < waitingVectors.size(); i++) {
                waitingReceivers.get(i).accept(new Stratum(name, join(waitingVectors.get(i))));
            }
            waitingVectors.clear();
            waitingReceivers.clear();
        }

        private int join(float[] vector) {
            int cluster = nearest(vector);
            counts[cluster]++;
            total++;
            float[] centroid = centroids[cluster];
            double step = 1.0 / counts[cluster];
            for (int i = 0; i < vector.length; i++) {
                centroid[i] += (float) ((vector[i] - centroid[i]) * step);
            }
            return cluster;
        }

        private void fit(Random random) {
            int queries = waitingVectors.size();
            double[] distance = new double[queries];
            Arrays.fill(distance, Double.MAX_VALUE);
            int chosen = random.nextInt(queries);
            for (int c = 0; c < centroids.length; c++) {
                System.arraycopy(waitingVectors.get(chosen), 0, centroids[c], 0, centroids[c].length);
                double sum = 0;
                for (int q = 0; q < queries; q++) {
                    distance[q] = Math.min(distance[q], VectorUtil.squareDistance(waitingVectors.get(q), centroids[c]));
                    sum += distance[q];
                }
                if (c + 1 < centroids.length) {
                    chosen = pick(distance, sum, random);
                }
            }
            int dims = centroids[0].length;
            for (int round = 0; round < FIT_ROUNDS; round++) {
                double[][] sums = new double[centroids.length][dims];
                long[] members = new long[centroids.length];
                for (float[] vector : waitingVectors) {
                    int cluster = nearest(vector);
                    members[cluster]++;
                    for (int i = 0; i < dims; i++) {
                        sums[cluster][i] += vector[i];
                    }
                }
                for (int c = 0; c < centroids.length; c++) {
                    if (members[c] > 0) { // a cluster that lost all its queries stays where it is
                        for (int i = 0; i < dims; i++) {
                            centroids[c][i] = (float) (sums[c][i] / members[c]);
                        }
                    }
                }
            }
        }

        /** Chosen in proportion to distance; any query if all are on a centroid already. */
        private static int pick(double[] distance, double sum, Random random) {
            if (sum <= 0) {
                return random.nextInt(distance.length);
            }
            double target = random.nextDouble() * sum;
            for (int q = 0; q < distance.length; q++) {
                target -= distance[q];
                if (target < 0) {
                    return q;
                }
            }
            return distance.length - 1;
        }

        double averageCount() {
            return (double) total / counts.length;
        }

        private int nearest(float[] vector) {
            int best = 0;
            double bestDistance = Double.MAX_VALUE;
            for (int c = 0; c < centroids.length; c++) {
                double distance = VectorUtil.squareDistance(vector, centroids[c]);
                if (distance < bestDistance) {
                    bestDistance = distance;
                    best = c;
                }
            }
            return best;
        }
    }
}
