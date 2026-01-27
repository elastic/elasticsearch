/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.lance.storage;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Core interface for Lance vector datasets.
 * <p>
 * This interface abstracts the Lance storage layer to support:
 * <ul>
 *   <li>FakeLanceDataset: In-memory implementation for testing</li>
 *   <li>RealLanceDataset: Production implementation using Lance SDK (when available)</li>
 * </ul>
 * <p>
 * The interface is designed to be extensible - additional methods for sharding,
 * metrics, health checks, etc. can be added in future implementations.
 */
public interface LanceDataset extends Closeable {

    /**
     * A search result candidate with document ID and similarity score.
     */
    record Candidate(String id, float score) {}

    /**
     * Get the vector dimensionality of this dataset.
     */
    int dims();

    /**
     * Get the number of vectors in this dataset.
     * Returns -1 if size is unknown.
     */
    default long size() {
        return -1;
    }

    /**
     * Check if the dataset has an index (e.g., IVF-PQ).
     * Non-indexed datasets use brute-force search.
     */
    default boolean hasIndex() {
        return false;
    }

    /**
     * Perform approximate nearest neighbor search.
     *
     * @param query Query vector (must match dims())
     * @param numCandidates Number of candidates to return
     * @param similarity Similarity metric: "cosine", "dot_product", or "l2"
     * @return List of candidates sorted by score (highest first)
     */
    List<Candidate> search(float[] query, int numCandidates, String similarity);

    /**
     * Get the URI this dataset was loaded from.
     */
    String uri();

    /**
     * Close this dataset and release resources.
     */
    @Override
    default void close() throws IOException {
        // Default no-op for simple implementations
    }
}
