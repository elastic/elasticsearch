/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

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
 * Keeps the sample from being made of the dense parts of the query space only. Queries are traffic, and traffic
 * has hotspots: a sample that follows it has many queries about the same few things and none about the rest, where
 * the recall may be quite different.
 * <p>
 * The query vectors of a space are grouped in a fixed number of clusters. The first distinct queries are kept
 * until there are enough to fit the clusters on, by k-means with k-means++ seeding. Taking the first queries as the
 * centroids instead, and moving them as the next ones join, does not work on embeddings: they are all alike, one
 * centroid ends up nearest to nearly every query and the others to hardly any. A query that arrives before the fit
 * has no cluster, and the sampler leaves it alone, until the fit gives it one. After the fit each new query joins the
 * nearest cluster and moves its centroid a little towards itself.
 * <p>
 * The clusters count the distinct queries that joined them. A query of a cluster with fewer than the average is
 * picked more often, by the factor {@code (average / count)^β}, and one of a cluster with more is picked less often.
 * With a balance β of 0 there is no effect, and with 1 every cluster is expected to have as many picks, whatever its
 * share of the queries. Replays of recorded traffic showed no gain from a balance near 1 where the recall does not
 * differ between the regions, and a loss of precision from the unequal weights, so it is meant to be used mildly.
 * <p>
 * The factor is worked out from the counts at the moment of the arrival and recorded in the probability of the
 * draw, like any other factor, so that the estimates stay right.
 */
public final class SpatialStrata {

    /**
     * The most a probability is raised by, so that a cluster that is hardly known does not make everything in it a sure pick.
     */
    static final double MAX_FACTOR = 10.0;

    /**
     * The most spaces, which are fields, that are kept track of. A query of any other is not given a cluster.
     */
    static final int MAX_SPACES = 100;

    /**
     * The fewest queries that are collected before the clusters are fitted.
     */
    static final int MIN_WARMUP = 200;

    /**
     * Rounds of reassigning the queries to the centroids and moving them, which is enough for the clusters to settle.
     */
    static final int FIT_ROUNDS = 5;

    private final int clusters;
    private final int warmup;
    private final Supplier<Random> random;
    private final Map<String, Space> spaces = new HashMap<>();
    private volatile double balance;

    /**
     * @param clusters the number of clusters of each space, 0 to not group the queries at all
     */
    public SpatialStrata(int clusters) {
        this(clusters, Math.max(MIN_WARMUP, 10 * clusters), Randomness::get);
    }

    /**
     * @param warmup the number of distinct queries of a space that are collected before the clusters are fitted
     * @param random the source of the choices of the seeding, which is looked up when it is needed
     */
    public SpatialStrata(int clusters, int warmup, Supplier<Random> random) {
        assert warmup >= clusters : "there have to be as many queries as clusters to make them from";
        this.clusters = clusters;
        this.warmup = warmup;
        this.random = random;
    }

    /**
     * Follows the balance setting, now and when it changes.
     */
    public void watch(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(QuerySamplingSettings.SPATIAL_BALANCE, value -> this.balance = value);
    }

    /**
     * Takes in the vector of a query that has not been seen before and tells which cluster it is in. If the clusters
     * are not fitted yet it is told later, when they are, which is also when the queries that were waiting are told.
     * A query that cannot be given a cluster is never told: no clusters are wanted, the vector is not made of finite
     * numbers, or there are too many spaces.
     *
     * @param receiver is called, on this thread, with the cluster of the query
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
        synchronized (spaces) {
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
    }

    /**
     * What the probability of picking a query of the stratum is multiplied with.
     */
    public double factor(@Nullable Stratum stratum) {
        double balance = this.balance;
        if (stratum == null || balance == 0.0) {
            return 1.0;
        }
        synchronized (spaces) {
            Space space = spaces.get(stratum.space());
            if (space == null) {
                return 1.0;
            }
            return Math.min(MAX_FACTOR, Math.pow(space.averageCount() / space.counts[stratum.cluster()], balance));
        }
    }

    /**
     * The distinct queries that joined each cluster of a space, for diagnostics and tests. Empty until the clusters are fitted.
     */
    public long[] counts(String space) {
        synchronized (spaces) {
            Space found = spaces.get(space);
            return found == null || found.fitted == false ? new long[0] : Arrays.copyOf(found.counts, found.counts.length);
        }
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

        /**
         * Puts a query in the nearest cluster, which takes it in.
         */
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

        /**
         * k-means++ seeding, in which a query is the next centroid with a chance that follows how far it is from the
         * centroids there are, and then rounds of moving each centroid to the mean of the queries nearest to it.
         */
        private void fit(Random random) {
            int queries = waitingVectors.size();
            double[] distance = new double[queries];
            Arrays.fill(distance, Double.MAX_VALUE);
            int chosen = random.nextInt(queries);
            for (int c = 0; c < centroids.length; c++) {
                System.arraycopy(waitingVectors.get(chosen), 0, centroids[c], 0, centroids[c].length);
                double sum = 0;
                for (int q = 0; q < queries; q++) {
                    distance[q] = Math.min(distance[q], squaredDistance(waitingVectors.get(q), centroids[c]));
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

        /**
         * A query chosen with a chance in proportion to its distance. If all are on the centroids already, any.
         */
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
                double distance = squaredDistance(vector, centroids[c]);
                if (distance < bestDistance) {
                    bestDistance = distance;
                    best = c;
                }
            }
            return best;
        }

        private static double squaredDistance(float[] a, float[] b) {
            double distance = 0;
            for (int i = 0; i < a.length; i++) {
                double diff = a[i] - b[i];
                distance += diff * diff;
            }
            return distance;
        }
    }
}
