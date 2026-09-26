/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

/**
 * A {@code KMeansResult} with pre-computed neighbor information
 */
public record KMeansNeighbors<V>(KMeansResult<V> result, NeighborHood[] neighborHoods) {

    public V[] centroids() {
        return result.centroids();
    }

    public int[] assignments() {
        return result.assignments();
    }
}
