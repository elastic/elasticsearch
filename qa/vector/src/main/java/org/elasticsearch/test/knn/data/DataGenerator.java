/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn.data;

import org.apache.lucene.search.Sort;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.test.knn.IndexVectorReader;
import org.elasticsearch.test.knn.KnnIndexTester;
import org.elasticsearch.test.knn.KnnSearcher;
import org.elasticsearch.test.knn.SearchParameters;

import java.io.IOException;

/**
 * Provides data for both indexing and searching phases of KNN benchmarking.
 * Each {@link DatasetConfig} variant creates the appropriate implementation
 * via {@link DatasetConfig#createDataGenerator}, eliminating the need
 * for instanceof checks in the orchestration code.
 */
public abstract sealed class DataGenerator permits PartitionDataGenerator, NonPartitionDataGenerator {

    private final IOSupplier<IndexVectorReader> docs;
    private final IOSupplier<IndexVectorReader> queries;
    private final int numDocs;
    private final int numQueries;

    public DataGenerator(IOSupplier<IndexVectorReader> docs, int numDocs, IOSupplier<IndexVectorReader> queries, int numQueries) {
        this.docs = docs;
        this.queries = queries;
        this.numDocs = numDocs;
        this.numQueries = numQueries;
    }

    /**
     *  vector reader for indexing.
     */
    public final IndexVectorReader docs() throws IOException {
        return docs.get();
    }

    /**
     *  number of documents to index, which may be less than the total number of vectors in the reader if we want to limit indexing time.
     */
    public final int numDocs() {
        return numDocs;
    }

    /**
     *  vector reader for searching.
     */
    public final IndexVectorReader queries() throws IOException {
        return queries.get();
    }

    /**
     *  number of search queries.
     */
    public final int numQueries() {
        return numQueries;
    }

    /**
     * Creates the indexing setup (vector reader, document factory, doc count) for this dataset.
     */
    public abstract KnnIndexTester.IndexingSetup createIndexingSetup() throws IOException;

    /**
     * Creates the search setup (query vectors, filter provider, results consumer) for this dataset.
     */
    public abstract KnnSearcher.SearchSetup createSearchSetup(KnnSearcher searcher, SearchParameters searchParameters) throws IOException;

    /**
     * Returns the index sort or null if these data does not require one.
     */
    public abstract Sort getIndexSort();
}
