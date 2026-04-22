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
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.search.vectors.PostFilterKnnQuery.POST_FILTERING_THRESHOLD;

/**
 * Interface for KNN queries that support post-filtering with retry.
 * Implemented by both HNSW ({@link ESKnnFloatVectorQuery}, {@link ESKnnByteVectorQuery})
 * and IVF ({@link IVFKnnFloatVectorQuery}) queries.
 * <p>
 */
public interface PostFilterableKnnQuery {

    /**
     * Creates a new query for the next retry round, configured to avoid re-visiting
     * previously seen results. For HNSW, this excludes previously seen doc IDs via a
     * FixedBitSet filter and seeds the next search with {@code previousResults}.
     * For IVF, this skips previously visited centroid posting lists.
     */
    Query createInnerQuery(IndexReader reader, int[] docsVisited);

    /**
     * Creates a filter-less delegate query for post-filtering. Subclasses provide
     * the concrete query type with the appropriate vector data.
     */
    PostFilterableKnnQuery createPostFilterDelegate(float filterSelectivity);

    long vectorOpsCount();

    int countTotalVectors(List<LeafReaderContext> leaves) throws IOException;

    @FunctionalInterface
    interface VectorCountSupplier {
        long totalVectors(LeafReaderContext ctx) throws IOException;
    }

    /**
     * Builds the cumulative seenDocs bitset from previous rounds' results, used by HNSW retry
     * to avoid re-visiting documents.
     */
    default FixedBitSet appendSeenDocs(FixedBitSet seenDocs, int[] previousResults, int maxDoc) {
        if (seenDocs == null) {
            seenDocs = new FixedBitSet(maxDoc);
        }
        if (previousResults != null) {
            for (int doc : previousResults) {
                if (doc >= 0 && doc < seenDocs.length()) {
                    seenDocs.set(doc);
                }
            }
        }
        return seenDocs;
    }

    default float computeSelectivity(Weight filterWeight, List<LeafReaderContext> leaves, int totalVectors) throws IOException {
        long filterCost = 0;
        for (LeafReaderContext leafCtx : leaves) {
            ScorerSupplier ss = filterWeight.scorerSupplier(leafCtx);
            if (ss != null) {
                filterCost += ss.cost();
            }
        }
        return totalVectors > 0 ? Math.min(1f, (float) filterCost / totalVectors) : 0f;
    }

    default Query createPostFilterQuery(
        List<LeafReaderContext> leaves,
        Weight filterWeight,
        int k,
        IndexReader reader,
        BitSetProducer parentsBitset
    ) throws IOException {
        int totalVectors = countTotalVectors(leaves);
        float selectivity = computeSelectivity(filterWeight, leaves, totalVectors);
        if (selectivity >= POST_FILTERING_THRESHOLD) {
            PostFilterableKnnQuery delegate = createPostFilterDelegate(selectivity);
            return new PostFilterKnnQuery(delegate, filterWeight, k, reader, parentsBitset);
        }
        return null;
    }

}
