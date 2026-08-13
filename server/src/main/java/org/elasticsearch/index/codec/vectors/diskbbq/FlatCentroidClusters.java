/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.elasticsearch.index.codec.vectors.cluster.KMeansResult;

/**
 * Information on a single flat layer of centroids with assignments
 */
public class FlatCentroidClusters implements CentroidIndex {
    private final KMeansResult<float[]> result;

    public FlatCentroidClusters(KMeansResult<float[]> result) {
        this.result = result;
    }

    @Override
    public boolean hasData() {
        return result.centroids().length > 1;
    }

    public int size() {
        return result.centroids().length;
    }

    public float[] getCentroid(int vectorOrdinal) {
        return result.getCentroid(vectorOrdinal);
    }

    public float[][] centroids() {
        return result.centroids();
    }

    public int[] assignments() {
        return result.assignments();
    }
}
