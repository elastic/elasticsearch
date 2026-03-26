/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn.data;

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
public interface DataGenerator {

    /**
     * Creates the indexing setup (vector reader, document factory, doc count, sort) for this dataset.
     */
    KnnIndexTester.IndexingSetup createIndexingSetup() throws IOException;

    /**
     * Creates the search setup (query vectors, filter provider, results consumer) for this dataset.
     */
    KnnSearcher.SearchSetup createSearchSetup(KnnSearcher searcher, SearchParameters searchParameters) throws IOException;

    /**
     * Whether this generator can provide query vectors for searching.
     */
    boolean hasQueries();
}
