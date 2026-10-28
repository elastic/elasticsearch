/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.index.FloatVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.lucene.util.VectorUtil.l2normalize;

/**
 * Synthetic vector fixtures for auto-calibration tests.
 */
public final class AutoCalibrationVectorFixtures {

    private AutoCalibrationVectorFixtures() {}

    static FloatVectorValues clusteredHeapVectors(int count, int dim, int numClusters, long seed) throws IOException {
        Random rnd = new Random(seed);
        float[][] centroids = new float[numClusters][dim];
        for (int c = 0; c < numClusters; c++) {
            for (int d = 0; d < dim; d++) {
                centroids[c][d] = rnd.nextFloat();
            }
            l2normalize(centroids[c]);
        }
        List<float[]> vecs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            float[] v = centroids[i % numClusters].clone();
            for (int d = 0; d < dim; d++) {
                v[d] += (rnd.nextFloat() - 0.5f) * 0.05f;
            }
            l2normalize(v);
            vecs.add(v);
        }
        return KMeansFloatVectorValues.build(vecs, null, dim);
    }

    static FloatVectorValues failingHeapVectors(int count, int dim, long seed) throws IOException {
        FloatVectorValues delegate = clusteredHeapVectors(count, dim, 16, seed);
        return new FloatVectorValues() {
            @Override
            public float[] vectorValue(int ord) throws IOException {
                throw new IOException("simulated calibration failure");
            }

            @Override
            public FloatVectorValues copy() throws IOException {
                return delegate.copy();
            }

            @Override
            public int dimension() {
                return delegate.dimension();
            }

            @Override
            public int size() {
                return delegate.size();
            }

            @Override
            public DocIndexIterator iterator() {
                return delegate.iterator();
            }
        };
    }
}
