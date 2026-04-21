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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
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
     * Finds raw (unfiltered) candidate results by executing this query's vector search.
     * Each implementation owns the full rewrite-and-extract lifecycle:
     * HNSW calls {@code searcher.rewrite(this)} and returns internally captured results
     * (Lucene's DocAndScoreQuery is package-private); IVF calls {@code searcher.rewrite(this)}
     * and extracts docs/scores from the returned {@link KnnScoreDocQuery}.
     */
    ScoreDoc[] findCandidates(IndexSearcher searcher) throws IOException;

    /**
     * Creates a new query for the next retry round, configured to avoid re-visiting
     * previously seen results. For HNSW, this excludes previously seen doc IDs via a
     * FixedBitSet filter and seeds the next search with {@code previousResults}.
     * For IVF, this skips previously visited centroid posting lists.
     *
     * @param previousResults the raw results from the current round, used by HNSW
     *                        to build the seen-docs exclusion set and seed the next search
     */
    PostFilterableKnnQuery createRetryQuery(IndexReader reader, ScoreDoc[] previousResults);

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
        PostFilterableKnnQuery create(int scaledK, int scaledNumCands, KnnSearchStrategy strategy, boolean earlyTermination);
    }
}
