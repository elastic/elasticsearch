/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.elasticsearch.index.codec.vectors.diskbbq.CentroidSupplier;

public interface Clusters {
    float[] getCentroid(int vectorOrdinal);

    CentroidSupplier centroids();

    int[] assignments();

    int[] secondaryAssignments();

    static Clusters empty() {
        return new Clusters() {
            @Override
            public float[] getCentroid(int vectorOrdinal) {
                return null;
            }

            @Override
            public float[][] centroids() {
                return new float[0][0];
            }

            @Override
            public int[] assignments() {
                return new int[0];
            }

            @Override
            public int[] secondaryAssignments() {
                return new int[0];
            }
        };
    }

    class SingleCluster implements Clusters {
        private final float[][] centroid;

        public SingleCluster(float[] centroid) {
            this.centroid = new float[][] { centroid };
        }

        @Override
        public float[] getCentroid(int vectorOrdinal) {
            return centroid[0];
        }

        @Override
        public float[][] centroids() {
            return centroid;
        }

        @Override
        public int[] assignments() {
            return null;
        }

        @Override
        public int[] secondaryAssignments() {
            return null;
        }
    }
}
