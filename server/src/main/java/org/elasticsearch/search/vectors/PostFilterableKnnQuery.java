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
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.knn.KnnSearchStrategy;

import java.io.IOException;

/**
 * Interface for KNN queries that support post-filtering with retry.
 * Implemented by both HNSW ({@link ESKnnFloatVectorQuery}, {@link ESKnnByteVectorQuery})
 * and IVF ({@link IVFKnnFloatVectorQuery}) queries.
 * <p>
 * Shared logic (selectivity computation, retry state building, collector wrapping) lives in
 * {@link PostFilterHelper} to keep this interface focused on the per-query-type contract.
 */
public interface PostFilterableKnnQuery {

    /**
     * Returns the raw (unfiltered) results captured during the most recent search execution.
     */
    TopDocs capturedResults();

    /**
     * Creates a new query for the next retry round, configured to avoid re-visiting
     * previously seen results. For HNSW, this excludes previously seen doc IDs via a
     * FixedBitSet filter. For IVF, this skips previously visited centroid posting lists.
     */
    PostFilterableKnnQuery createRetryQuery(IndexReader reader);

    /**
     * Returns the number of vector operations performed during the most recent search.
     */
    long vectorOpsCount();

    @FunctionalInterface
    interface VectorCountSupplier {
        long totalVectors(LeafReaderContext ctx) throws IOException;
    }

    /** Creates a filter-less delegate for post-filtering — type-specific. */
    @FunctionalInterface
    interface PostFilterDelegateFactory {
        PostFilterableKnnQuery create(int scaledNumCands, KnnSearchStrategy strategy, boolean earlyTermination);
    }
}
