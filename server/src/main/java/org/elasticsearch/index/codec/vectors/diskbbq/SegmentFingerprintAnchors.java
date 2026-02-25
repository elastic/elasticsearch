/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;

import java.io.IOException;
import java.util.Random;

/**
 * Fixed set of anchor vectors used to compute segment and query fingerprints for IVF
 * allocation. Anchors are deterministic per dimension so index and query agree.
 * Segment fingerprint: for each anchor, max similarity to any segment centroid.
 * Query fingerprint: similarity of query to each anchor. Affinity is then derived
 * by combining the two (e.g. dot product) without scoring the query against segment centroids.
 */
public final class SegmentFingerprintAnchors {

    public static final int ancoraDirections = 8;

    private static final long SEED = 0x9E3779B97F4A7C15L;

    private SegmentFingerprintAnchors() {}

    /**
     * Returns K unit-norm anchor vectors for the given dimension (deterministic).
     */
    public static float[][] getAnchors(int dimension, VectorSimilarityFunction similarityFunction) {
        Random rng = new Random(SEED + dimension * 31L);
        float[][] anchors = new float[ancoraDirections][dimension];
        for (int j = 0; j < ancoraDirections; j++) {
            for (int d = 0; d < dimension; d++) {
                anchors[j][d] = (float) (rng.nextGaussian());
            }
            VectorUtil.l2normalize(anchors[j]);
        }
        return anchors;
    }

    /**
     * Compute segment fingerprint: for each anchor, max similarity to any centroid.
     */
    public static float[] computeSegmentFingerprint(
        float[][] centroids,
        VectorSimilarityFunction similarityFunction,
        float[][] anchors
    ) {
        float[] fp = new float[ancoraDirections];
        for (int j = 0; j < ancoraDirections; j++) {
            float maxSim = Float.NEGATIVE_INFINITY;
            for (float[] centroid : centroids) {
                float sim = similarityFunction.compare(anchors[j], centroid);
                if (sim > maxSim) {
                    maxSim = sim;
                }
            }
            fp[j] = maxSim == Float.NEGATIVE_INFINITY ? 0f : maxSim;
        }
        return fp;
    }

    /**
     * Compute segment fingerprint from a centroid supplier (e.g., at merge time).
     */
    public static float[] computeSegmentFingerprint(
        CentroidSupplier centroidSupplier,
        VectorSimilarityFunction similarityFunction,
        float[][] anchors
    ) throws IOException {
        int numCentroids = centroidSupplier.size();
        float[] fp = new float[ancoraDirections];
        for (int j = 0; j < ancoraDirections; j++) {
            float maxSim = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numCentroids; i++) {
                float sim = similarityFunction.compare(anchors[j], centroidSupplier.centroid(i));
                if (sim > maxSim) {
                    maxSim = sim;
                }
            }
            fp[j] = maxSim == Float.NEGATIVE_INFINITY ? 0f : maxSim;
        }
        return fp;
    }

    /**
     * Compute query fingerprint: similarity of query to each anchor.
     */
    public static float[] computeQueryFingerprint(
        float[] queryVector,
        VectorSimilarityFunction similarityFunction,
        float[][] anchors
    ) {
        float[] fp = new float[ancoraDirections];
        for (int j = 0; j < ancoraDirections; j++) {
            fp[j] = similarityFunction.compare(queryVector, anchors[j]);
        }
        return fp;
    }

    /**
     * Combine query and segment fingerprints into a single affinity score (e.g. dot product).
     */
    public static float affinityFromFingerprints(float[] queryFp, float[] segmentFp) {
        assert queryFp.length == segmentFp.length && queryFp.length == ancoraDirections;
        float dot = 0f;
        for (int j = 0; j < ancoraDirections; j++) {
            dot += queryFp[j] * segmentFp[j];
        }
        return Math.max(0f, dot);
    }
}
