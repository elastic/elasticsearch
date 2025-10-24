/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import java.io.IOException;

/**
 * An interface for that supply centroids.
 */
public interface CentroidSupplier {
    CentroidSupplier EMPTY = new CentroidSupplier() {
        @Override
        public int size() {
            return 0;
        }

        @Override
        public float[] centroid(int centroidOrdinal) {
            throw new IllegalStateException("No centroids");
        }
    };

    int size();

    float[] centroid(int centroidOrdinal) throws IOException;

    static CentroidSupplier fromArray(float[][] centroids) {
        if (centroids.length == 0) {
            return EMPTY;
        }
        return new CentroidSupplier() {
            @Override
            public int size() {
                return centroids.length;
            }

            @Override
            public float[] centroid(int centroidOrdinal) {
                return centroids[centroidOrdinal];
            }
        };
    }
}
