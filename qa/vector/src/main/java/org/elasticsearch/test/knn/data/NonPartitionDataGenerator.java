/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn.data;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.test.knn.IndexVectorReader;
import org.elasticsearch.test.knn.KnnIndexTester;
import org.elasticsearch.test.knn.KnnIndexer;
import org.elasticsearch.test.knn.KnnSearcher;
import org.elasticsearch.test.knn.SearchParameters;

import java.io.IOException;
import java.util.Random;

/**
 *  Generates non-partitioned vector data for KNN benchmarking.
 */
public final class NonPartitionDataGenerator extends DataGenerator {

    NonPartitionDataGenerator(IOSupplier<IndexVectorReader> docs, int numDocs, IOSupplier<IndexVectorReader> queries, int numQueries) {
        super(docs, numDocs, queries, numQueries);
    }

    @Override
    public KnnIndexTester.IndexingSetup createIndexingSetup() throws IOException {
        return new KnnIndexTester.IndexingSetup(docs(), new KnnIndexer.DefaultDocumentFactory(), numDocs());
    }

    @Override
    public KnnSearcher.SearchSetup createSearchSetup(KnnSearcher searcher, SearchParameters searchParameters) throws IOException {
        float[][] floatQueries = null;
        byte[][] byteQueries = null;
        IndexVectorReader targetReader = queries();
        switch (searcher.vectorEncoding()) {
            case BYTE -> {
                byteQueries = new byte[searcher.numQueryVectors()][];
                for (int i = 0; i < searcher.numQueryVectors(); i++) {
                    byteQueries[i] = targetReader.nextByteVector();
                }
            }
            case FLOAT32 -> {
                floatQueries = new float[searcher.numQueryVectors()][];
                for (int i = 0; i < searcher.numQueryVectors(); i++) {
                    floatQueries[i] = targetReader.nextFloatVector();
                }
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

        var provider = new KnnSearcher.SimpleFilterQueryProvider(numQueries(), selectivityFilter);
        var consumer = new KnnSearcher.FileBasedResultsConsumer(searcher, this, selectivityFilter);
        return new KnnSearcher.SearchSetup(floatQueries, byteQueries, provider, consumer);
    }

    @Override
    public Sort getIndexSort() {
        return null;
    }
}
