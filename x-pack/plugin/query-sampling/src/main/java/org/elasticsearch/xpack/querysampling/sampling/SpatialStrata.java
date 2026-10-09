/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;
import org.elasticsearch.xpack.querysampling.dedup.Stratum;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Keeps the sample from being made of the dense parts of the query space only. Queries are traffic, and traffic
 * has hotspots: a sample that follows it has many queries about the same few things and none about the rest, where
 * the recall may be quite different.
 * <p>
 * The query vectors are grouped in a fixed number of clusters, by sequential k-means: the first queries of a space
 * become its centroids, and each query after that joins the nearest one and moves it a little towards itself. The
 * clusters count the distinct queries that joined them. A query of a cluster with fewer than the average is picked
 * more often, by the factor {@code (average / count)^β}, and one of a cluster with more is picked less often. With a
 * balance β of 0 there is no effect, and with 1 every cluster is expected to have as many picks, whatever its
 * share of the queries.
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

    private final int clusters;
    private final Map<String, Space> spaces = new HashMap<>();
    private volatile double balance;

    /**
     * @param clusters the number of clusters of each space, 0 to not group the queries at all
     */
    public SpatialStrata(int clusters) {
        this.clusters = clusters;
    }

    /**
     * Follows the balance setting, now and when it changes.
     */
    public void watch(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(QuerySamplingSettings.SPATIAL_BALANCE, value -> this.balance = value);
    }

    /**
     * Puts the vector of a query that has not been seen before in a cluster, which takes in the query.
     *
     * @return the cluster, or null if there is none to give: no clusters are wanted, the vector is not made of
     *         finite numbers, or there are too many spaces
     */
    @Nullable
    public Stratum assign(String field, float[] vector) {
        if (clusters == 0 || vector.length == 0) {
            return null;
        }
        for (float value : vector) {
            if (Float.isFinite(value) == false) {
                return null; // it would take the centroid of the cluster that it joins with it
            }
        }
        String name = field + "/" + vector.length;
        synchronized (spaces) {
            Space space = spaces.get(name);
            if (space == null) {
                if (spaces.size() >= MAX_SPACES) {
                    return null;
                }
                space = new Space(clusters, vector.length);
                spaces.put(name, space);
            }
            return new Stratum(name, space.add(vector));
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
     * The distinct queries that joined each cluster of a space, for diagnostics and tests.
     */
    public long[] counts(String space) {
        synchronized (spaces) {
            Space found = spaces.get(space);
            return found == null ? new long[0] : Arrays.copyOf(found.counts, found.used);
        }
    }

    private static final class Space {
        private final float[][] centroids;
        private final long[] counts;
        private int used;
        private long total;

        Space(int clusters, int dims) {
            this.centroids = new float[clusters][dims];
            this.counts = new long[clusters];
        }

        int add(float[] vector) {
            int cluster;
            if (used < centroids.length) {
                cluster = used++; // the first queries are the centroids
                System.arraycopy(vector, 0, centroids[cluster], 0, vector.length);
            } else {
                cluster = nearest(vector);
            }
            counts[cluster]++;
            total++;
            float[] centroid = centroids[cluster];
            double step = 1.0 / counts[cluster];
            for (int i = 0; i < vector.length; i++) {
                centroid[i] += (float) ((vector[i] - centroid[i]) * step);
            }
            return cluster;
        }

        double averageCount() {
            return (double) total / used;
        }

        private int nearest(float[] vector) {
            int best = 0;
            double bestDistance = Double.MAX_VALUE;
            for (int c = 0; c < used; c++) {
                float[] centroid = centroids[c];
                double distance = 0;
                for (int i = 0; i < vector.length; i++) {
                    double diff = vector[i] - centroid[i];
                    distance += diff * diff;
                }
                if (distance < bestDistance) {
                    bestDistance = distance;
                    best = c;
                }
            }
            return best;
        }
    }
}
