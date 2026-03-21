/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn;

import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.elasticsearch.test.knn.KnnIndexTester.logger;

/**
 * Generates synthetic partitioned vector data for KNN benchmarking.
 * Documents are distributed across partitions according to a configurable distribution,
 * and each document is assigned a partition_id and a random vector.
 * Implements {@link DataGenerator} to provide unified indexing and search setup.
 */
class PartitionDataGenerator implements DataGenerator {

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

    @Override
    public KnnIndexTester.IndexingSetup createIndexingSetup() {
        var assignments = getPartitionAssignments();
        int totalDocs = 0;
        for (var docIds : assignments.values()) {
            totalDocs += docIds.size();
        }
        String[] docPartitionIds = new String[totalDocs];
        int[] docOrdinals = new int[totalDocs];
        int idx = 0;
        for (var entry : assignments.entrySet()) {
            for (int docId : entry.getValue()) {
                docPartitionIds[idx] = entry.getKey();
                docOrdinals[idx] = docId;
                idx++;
            }
        }
        logger.info("IndexingSetup: generated data with {} partitions, dim={}", this.numPartitions, this.dimensions);
        KnnIndexer.IndexVectorReader vectorReader = new KnnIndexer.PartitionGeneratingVectorReader(this);
        KnnIndexer.DocumentFactory documentFactory = new KnnIndexer.PartitionDocumentFactory(docPartitionIds, docOrdinals);
        Sort indexSort = new Sort(new SortField(KnnIndexer.PARTITION_ID_FIELD, SortField.Type.STRING, false));
        return new KnnIndexTester.IndexingSetup(vectorReader, documentFactory, totalDocs, indexSort);
    }

    @Override
    public KnnSearcher.SearchSetup createSearchSetup(KnnSearcher searcher, SearchParameters searchParameters) throws IOException {
        float[][] floatQueries = null;
        byte[][] byteQueries = null;

        if (searcher.vectorEncoding.equals(VectorEncoding.BYTE)) {
            byteQueries = generateQueryByteVectors(searcher.numQueryVectors);
        } else {
            floatQueries = generateQueryVectors(searcher.numQueryVectors);
        }

        Query selectivityFilter = searchParameters.filterSelectivity() < 1f
            ? KnnSearcher.generateRandomQuery(
                new Random(searchParameters.seed()),
                searcher.indexPath,
                searcher.numDocs,
                searchParameters.filterSelectivity(),
                searchParameters.filterCached()
            )
            : null;

        List<String> partitionIds = new ArrayList<>(getPartitionAssignments().keySet());
        Random queryRandom = new Random(searchParameters.seed());
        int numSampledPartitions = Math.min(partitionIds.size(), 10);
        List<String> sampledPartitions = new ArrayList<>(partitionIds);
        Collections.shuffle(sampledPartitions, queryRandom);
        sampledPartitions = sampledPartitions.subList(0, numSampledPartitions);
        logger.info("Sampled {} of {} partitions for search", numSampledPartitions, partitionIds.size());

        var provider = new KnnSearcher.PartitionFilterQueryProvider(sampledPartitions, searcher.numQueryVectors, selectivityFilter);
        var consumer = new KnnSearcher.PartitionResultsConsumer(
            searcher.indexPath,
            searcher.vectorEncoding,
            searcher.similarityFunction,
            searcher.numQueryVectors,
            provider,
            selectivityFilter,
            floatQueries,
            byteQueries
        );
        return new KnnSearcher.SearchSetup(floatQueries, byteQueries, provider, consumer);
    }

    @Override
    public boolean hasQueries() {
        return true;
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
