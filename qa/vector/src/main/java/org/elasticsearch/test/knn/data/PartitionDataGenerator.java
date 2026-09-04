/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn.data;

import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.test.knn.IndexVectorReader;
import org.elasticsearch.test.knn.KnnIndexTester;
import org.elasticsearch.test.knn.KnnIndexer;
import org.elasticsearch.test.knn.KnnSearcher;
import org.elasticsearch.test.knn.SearchParameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.elasticsearch.test.knn.KnnIndexTester.logger;

/**
 * Generates partitioned vector data for KNN benchmarking.
 * Documents are distributed across partitions according to a configurable distribution,
 * and each document is assigned a partition_id and a random vector.
 */
public final class PartitionDataGenerator extends DataGenerator {

    private final PartitionConfiguration partitionConfiguration;

    PartitionDataGenerator(
        IOSupplier<IndexVectorReader> docs,
        int numDocs,
        IOSupplier<IndexVectorReader> queries,
        int numQueries,
        PartitionConfiguration partitionConfiguration
    ) {
        super(docs, numDocs, queries, numQueries);
        this.partitionConfiguration = partitionConfiguration;
    }

    /**
     * Returns the partition assignments: a map from partition_id to the list of document ordinals assigned to that partition.
     * Documents are assigned contiguously per partition (sorted by partition_id) to facilitate index sorting.
     */
    Map<String, List<Integer>> getPartitionAssignments() {
        return partitionConfiguration.assignmentInfo().partitionToDocIds();
    }

    /**
     * Returns the number of partitions.
     */
    int getNumPartitions() {
        return partitionConfiguration.numPartitions();
    }

    @Override
    public KnnIndexTester.IndexingSetup createIndexingSetup() throws IOException {
        int totalDocs = partitionConfiguration.assignmentInfo().docOrdinals().length;
        logger.info("IndexingSetup: generated data with {} partitions", getNumPartitions());
        KnnIndexer.DocumentFactory documentFactory = new KnnIndexer.PartitionDocumentFactory(
            partitionConfiguration.assignmentInfo().docPartitionIds(),
            partitionConfiguration.assignmentInfo().docOrdinals()
        );
        return new KnnIndexTester.IndexingSetup(docs(), documentFactory, totalDocs);
    }

    @Override
    public KnnSearcher.SearchSetup createSearchSetup(KnnSearcher searcher, SearchParameters searchParameters) throws IOException {
        float[][] floatQueries = null;
        byte[][] byteQueries = null;

        IndexVectorReader targetReader = queries();
        if (searcher.vectorEncoding().equals(VectorEncoding.BYTE)) {
            byteQueries = new byte[numQueries()][];
            for (int i = 0; i < numQueries(); i++) {
                byteQueries[i] = targetReader.nextByteVector().vector();
            }
        } else {
            floatQueries = new float[numQueries()][];
            for (int i = 0; i < numQueries(); i++) {
                floatQueries[i] = targetReader.nextFloatVector().vector();
            }
        }

        Query selectivityFilter = searchParameters.filterSelectivity() < 1f
            ? KnnSearcher.generateRandomQuery(
                new Random(searchParameters.seed()),
                searcher.indexPath(),
                searcher.numDocs(),
                searchParameters.filterSelectivity(),
                searchParameters.filterCached()
            )
            : null;

        List<String> sampledPartitions = new ArrayList<>(getPartitionAssignments().keySet());
        Random queryRandom = new Random(searchParameters.seed());
        int numSampledPartitions = Math.min(sampledPartitions.size(), 10);
        Collections.shuffle(sampledPartitions, queryRandom);
        sampledPartitions = sampledPartitions.subList(0, numSampledPartitions);
        logger.info("Sampled {} of {} partitions for search", numSampledPartitions, getPartitionAssignments().size());

        var provider = new KnnSearcher.PartitionFilterQueryProvider(sampledPartitions, searcher.numQueryVectors(), selectivityFilter);
        var consumer = new KnnSearcher.PartitionResultsConsumer(searcher, this, selectivityFilter, provider);
        return new KnnSearcher.SearchSetup(floatQueries, byteQueries, provider, consumer);
    }

    @Override
    public Sort getIndexSort() {
        return new Sort(new SortField(KnnIndexer.PARTITION_ID_FIELD, SortField.Type.STRING, false));
    }
}
