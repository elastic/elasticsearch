/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.util.List;

/**
 * Interface for KNN queries that support post-filtering with retry.
 * Implemented by both HNSW ({@link ESKnnFloatVectorQuery}, {@link ESKnnByteVectorQuery})
 * and IVF ({@link IVFKnnFloatVectorQuery}) queries.
 */
public interface PostFilterableKnnQuery {

    /**
     * Creates a new query for the next retry round, configured to avoid re-visiting
     * previously seen results. For HNSW, this excludes previously seen doc IDs via
     * {@link ExcludeDocsQuery} and seeds the next search with the same doc IDs.
     * For IVF, this skips previously visited centroid posting lists.
     *
     * @param reader      the index reader
     * @param allSeenDocs sorted array of ALL doc IDs seen across all previous rounds
     */
    Query createRetryQuery(IndexReader reader, int[] allSeenDocs);

    /**
     * Creates a filter-less delegate query for post-filtering. Subclasses provide
     * the concrete query type with the appropriate vector data.
     */
    Query createPostFilterDelegate(float filterSelectivity);

    int countTotalVectors(List<LeafReaderContext> leaves) throws IOException;

    long totalVectorOps();
}
