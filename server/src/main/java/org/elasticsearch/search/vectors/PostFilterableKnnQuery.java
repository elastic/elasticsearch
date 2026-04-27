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
     * Safety multiplier applied on top of the {@code 1/selectivity} oversample when sizing
     * the first post-filter round. Compensates for statistical fluctuation in how many
     * HNSW candidates pass the filter, reducing the probability that round 1 falls short
     * of {@code k} and forces a full retry round.
     */
    float POST_FILTER_OVERSAMPLE_SAFETY_FACTOR = 1.2f;

    /**
     * Creates a new query for the next retry round.
     * <p>
     * For HNSW: {@code excludedDocs} becomes an {@link ExcludeDocsQuery} filter (which Lucene's
     * {@code AbstractKnnVectorQuery#rewrite} converts into {@code AcceptDocs}), and {@code seedDocs}
     * (filter-passing docs only) feed the {@link SeededRetryCollectorManager} as graph entry points.
     * <p>
     * For IVF: {@code excludedDocs} are composed into {@code AcceptDocs} so the codec skips them
     * during posting-list iteration; {@code seedDocs} are ignored (IVF has no graph seeding).
     *
     * @param reader           the index reader
     * @param excludedDocs     all docs returned across previous rounds, sorted (skip from results)
     * @param seedDocs         filter-passing docs from previous rounds, sorted (HNSW seeding only)
     * @param requestK         how many top results to keep in the retry's collector heap
     * @param requestNumCands  KNN beam width / collector candidate budget for the retry
     */
    Query createRetryQuery(IndexReader reader, int[] excludedDocs, int[] seedDocs, int requestK, int requestNumCands);

    /**
     * Creates a filter-less delegate query for post-filtering. Subclasses provide
     * the concrete query type with the appropriate vector data.
     */
    Query createPostFilterDelegate(float filterSelectivity);

    int countTotalVectors(List<LeafReaderContext> leaves) throws IOException;

    long totalVectorOps();

    int k();

    /**
     * @return the current numCands (KNN beam / collector candidate budget) for this query.
     * Used by {@link PostFilterKnnQuery} to compute retry-round numCands scaling.
     */
    int numCands();
}
