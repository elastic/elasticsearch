/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.vectors.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans.NO_SOAR_ASSIGNMENT;

public class FloatHierarchicalKMeansTests extends AbstractHierarchicalKMeansTestCase<float[]> {

    @Override
    protected CentroidOps<float[]> centroidOps() {
        return CentroidOps.FLOAT;
    }

    @Override
    protected ClusteringVectorValues<float[]> generateData(int nSamples, int nDims, int nClusters) {
        return KMeansTestData.generateFloatData(nSamples, nDims, nClusters);
    }

    @Override
    protected ClusteringVectorValues<float[]> generateFewDistinctData(int nVectors, int dims, int diffValues) {
        float[][] values = new float[diffValues][dims];
        for (int i = 0; i < diffValues; i++) {
            for (int j = 0; j < dims; j++) {
                values[i][j] = random().nextFloat();
            }
        }
        List<float[]> vectorList = new ArrayList<>(nVectors);
        for (int i = 0; i < nVectors; i++) {
            vectorList.add(values[random().nextInt(diffValues)]);
        }
        return KMeansFloatVectorValues.build(vectorList, null, dims);
    }

    /**
     * Verify that SOAR assignments never collide with primary assignments after empty clusters
     * are removed. This exercises the neighborhood remapping in removeEmptyClusters: when empty
     * centroids are compacted out and neighbor indices are remapped, no neighbor should be mapped
     * to a vector's own primary centroid.
     *
     * The test creates a dataset with fewer natural clusters than what the algorithm targets,
     * uses a small clustersPerNeighborhood to force neighborhood-aware SOAR, and repeats across
     * random parameters to cover different empty-cluster scenarios.
     */
    public void testSoarAssignmentsValidAfterEmptyClusterRemoval() throws IOException {
        for (int trial = 0; trial < 200; trial++) {
            // Use few natural clusters but many vectors, so the algorithm over-partitions
            // and some clusters end up empty after refinement.
            int naturalClusters = randomIntBetween(2, 4);
            int nVectors = randomIntBetween(200, 1000);
            int dims = randomIntBetween(4, 32);

            KMeansFloatVectorValues vectors = KMeansTestData.generateFloatData(nVectors, dims, naturalClusters);

            // Small clustersPerNeighborhood ensures neighborhoods are active when centroids > this value
            int clustersPerNeighborhood = 2;
            // Very small target size forces many centroids, maximizing chance of empty clusters
            int targetSize = randomIntBetween(3, 10);
            float soarLambda = randomFloat() * 0.5f + 0.5f;
            // Low maxIterations increases chance of poorly-converged clusters that become empty
            int maxIterations = randomIntBetween(1, 5);

            HierarchicalKMeans<float[]> hkmeans = HierarchicalKMeans.ofSerial(
                CentroidOps.FLOAT,
                dims,
                maxIterations,
                randomIntBetween(50, nVectors),
                clustersPerNeighborhood,
                soarLambda
            );

            KMeansResult<float[]> result = hkmeans.cluster(vectors, targetSize);

            int[] assignments = result.assignments();
            int[] soarAssignments = result.soarAssignments();

            if (result.centroids().length > 1 && result.centroids().length < nVectors) {
                assertEquals(nVectors, soarAssignments.length);
                for (int i = 0; i < assignments.length; i++) {
                    int soar = soarAssignments[i];
                    if (soar != NO_SOAR_ASSIGNMENT) {
                        assertNotEquals(
                            "SOAR assignment collides with primary assignment for vector "
                                + i
                                + " (both assigned to centroid "
                                + assignments[i]
                                + ")",
                            assignments[i],
                            soar
                        );
                    }
                }
            }
        }
    }
}
