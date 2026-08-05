/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn.data;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.elasticsearch.test.knn.KnnIndexTester.logger;

public record PartitionConfiguration(int numPartitions, PartitionAssignmentInfo assignmentInfo) {

    record PartitionAssignmentInfo(Map<String, List<Integer>> partitionToDocIds, String[] docPartitionIds, int[] docOrdinals) {}

    public PartitionConfiguration(int numDocs, int numPartitions, DatasetConfig.PartitionDistribution distribution) {
        this(numPartitions, computePartitionAssignments(numDocs, numPartitions, distribution));
    }

    private static PartitionAssignmentInfo computePartitionAssignments(
        int numDocs,
        int numPartitions,
        DatasetConfig.PartitionDistribution distribution
    ) {
        int[] docsPerPartition = computeDocsPerPartition(numDocs, numPartitions, distribution);
        Map<String, List<Integer>> assignments = new LinkedHashMap<>();
        String[] docPartitionIds = new String[numDocs];
        int[] docOrdinals = new int[numDocs];
        int docOrd = 0;
        for (int t = 0; t < numPartitions; t++) {
            String partitionId = String.format("partition_%06d", t);
            final int startOrd = docOrd;
            List<Integer> docIds = IntStream.range(0, docsPerPartition[t]).map(d -> startOrd + d).boxed().toList();
            for (int docId : docIds) {
                docPartitionIds[docOrd] = partitionId;
                docOrdinals[docOrd] = docId;
                docOrd++;
            }
            assignments.put(partitionId, docIds);
        }
        logger.info("Generated partition assignments: {} partitions, {} total docs, distribution={}", numPartitions, docOrd, distribution);
        return new PartitionAssignmentInfo(assignments, docPartitionIds, docOrdinals);
    }

    private static int[] computeDocsPerPartition(int numDocs, int numPartitions, DatasetConfig.PartitionDistribution distribution) {
        return switch (distribution) {
            case UNIFORM -> computeUniform(numDocs, numPartitions);
            case ZIPF -> computeZipf(numDocs, numPartitions);
        };
    }

    private static int[] computeUniform(int numDocs, int numPartitions) {
        int[] counts = new int[numPartitions];
        int base = numDocs / numPartitions;
        int remainder = numDocs % numPartitions;
        Arrays.setAll(counts, i -> base + (i < remainder ? 1 : 0));
        return counts;
    }

    /**
     * Computes a Zipf distribution of documents across partitions.
     * The first partition gets the most documents, following a power-law decay.
     */
    private static int[] computeZipf(int numDocs, int numPartitions) {
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
