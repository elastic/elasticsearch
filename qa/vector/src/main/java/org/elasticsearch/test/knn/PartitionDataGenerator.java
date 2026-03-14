/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.elasticsearch.test.knn.KnnIndexTester.logger;

/**
 * Generates synthetic partitioned vector data for KNN benchmarking.
 * Documents are distributed across partitions according to a configurable distribution,
 * and each document is assigned a partition_id and a random vector.
 */
class PartitionDataGenerator {

    private final int numDocs;
    private final int dimensions;
    private final int numPartitions;
    private final String distribution;
    private final Random random;
    private final Map<String, List<Integer>> partitionAssignments;

    PartitionDataGenerator(int numDocs, int dimensions, int numPartitions, String distribution, long seed) {
        this.numDocs = numDocs;
        this.dimensions = dimensions;
        this.numPartitions = numPartitions;
        this.distribution = distribution;
        this.random = new Random(seed);
        this.partitionAssignments = computePartitionAssignments();
    }

    /**
     * Returns the partition_id for a given document ordinal.
     */
    String partitionId(int docOrd) {
        // Iterate through partition assignments to find the partition for this doc
        for (var entry : partitionAssignments.entrySet()) {
            if (entry.getValue().contains(docOrd)) {
                return entry.getKey();
            }
        }
        throw new IllegalStateException("doc " + docOrd + " not assigned to any partition");
    }

    /**
     * Returns the partition assignments: a map from partition_id to the list of document ordinals assigned to that partition.
     * Documents are assigned contiguously per partition (sorted by partition_id) to facilitate index sorting.
     */
    Map<String, List<Integer>> getPartitionAssignments() {
        return partitionAssignments;
    }

    /**
     * Returns the number of partitions.
     */
    int getNumPartitions() {
        return numPartitions;
    }

    /**
     * Generates a random float vector of the configured dimensions.
     */
    float[] nextVector() {
        float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = random.nextFloat() * 2 - 1; // uniform in [-1, 1]
        }
        return vector;
    }

    /**
     * Generates a random byte vector of the configured dimensions.
     */
    byte[] nextByteVector() {
        byte[] vector = new byte[dimensions];
        random.nextBytes(vector);
        return vector;
    }

    /**
     * Generates query vectors. These are random vectors not tied to any partition.
     */
    float[][] generateQueryVectors(int numQueries) {
        float[][] queries = new float[numQueries][dimensions];
        for (int i = 0; i < numQueries; i++) {
            queries[i] = nextVector();
        }
        return queries;
    }

    /**
     * Generates query byte vectors. These are random vectors not tied to any partition.
     */
    byte[][] generateQueryByteVectors(int numQueries) {
        byte[][] queries = new byte[numQueries][dimensions];
        for (int i = 0; i < numQueries; i++) {
            queries[i] = nextByteVector();
        }
        return queries;
    }

    private Map<String, List<Integer>> computePartitionAssignments() {
        int[] docsPerPartition = computeDocsPerPartition();
        Map<String, List<Integer>> assignments = new LinkedHashMap<>();
        int docOrd = 0;
        for (int t = 0; t < numPartitions; t++) {
            String partitionId = String.format("partition_%06d", t);
            List<Integer> docIds = new ArrayList<>(docsPerPartition[t]);
            for (int d = 0; d < docsPerPartition[t]; d++) {
                docIds.add(docOrd++);
            }
            assignments.put(partitionId, docIds);
        }
        logger.info("Generated partition assignments: {} partitions, {} total docs, distribution={}", numPartitions, docOrd, distribution);
        return assignments;
    }

    private int[] computeDocsPerPartition() {
        return switch (distribution) {
            case "uniform" -> computeUniform();
            case "zipf" -> computeZipf();
            default -> throw new IllegalArgumentException("Unknown partition distribution: " + distribution + ". Use 'uniform' or 'zipf'.");
        };
    }

    private int[] computeUniform() {
        int[] counts = new int[numPartitions];
        int base = numDocs / numPartitions;
        int remainder = numDocs % numPartitions;
        for (int i = 0; i < numPartitions; i++) {
            counts[i] = base + (i < remainder ? 1 : 0);
        }
        return counts;
    }

    /**
     * Computes a Zipf distribution of documents across partitions.
     * The first partition gets the most documents, following a power-law decay.
     */
    private int[] computeZipf() {
        double[] weights = new double[numPartitions];
        double totalWeight = 0;
        for (int i = 0; i < numPartitions; i++) {
            weights[i] = 1.0 / (i + 1); // Zipf: rank^-1
            totalWeight += weights[i];
        }
        int[] counts = new int[numPartitions];
        int assigned = 0;
        for (int i = 0; i < numPartitions; i++) {
            counts[i] = Math.max(1, (int) Math.round(numDocs * weights[i] / totalWeight));
            assigned += counts[i];
        }
        // Adjust for rounding errors
        int diff = numDocs - assigned;
        if (diff > 0) {
            // distribute remaining docs to the largest partitions
            for (int i = 0; i < diff; i++) {
                counts[i % numPartitions]++;
            }
        } else if (diff < 0) {
            // remove excess docs from the smallest partitions (from the end)
            for (int i = numPartitions - 1; i >= 0 && diff < 0; i--) {
                int remove = Math.min(-diff, counts[i] - 1);
                counts[i] -= remove;
                diff += remove;
            }
        }
        return counts;
    }
}
