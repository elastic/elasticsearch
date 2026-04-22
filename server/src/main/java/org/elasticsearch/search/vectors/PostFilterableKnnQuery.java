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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.search.vectors.PostFilterKnnQuery.POST_FILTERING_THRESHOLD;

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
    Query createInnerQuery(IndexReader reader, int[] allSeenDocs);

    /**
     * Creates a filter-less delegate query for post-filtering. Subclasses provide
     * the concrete query type with the appropriate vector data.
     */
    PostFilterableKnnQuery createPostFilterDelegate(float filterSelectivity);

    long vectorOpsCount();

    int countTotalVectors(List<LeafReaderContext> leaves) throws IOException;

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

    default Weight createFilterWeight(IndexSearcher searcher, Query filter, int[] seenDocs, String field) throws IOException {
        if (filter == null && seenDocs == null) {
            return null;
        }
        var booleanQueryBuilder = new BooleanQuery.Builder();
        if (filter != null) {
            booleanQueryBuilder = booleanQueryBuilder.add(filter, BooleanClause.Occur.FILTER);
        }
        ;
        if (seenDocs != null) {
            booleanQueryBuilder = booleanQueryBuilder.add(
                new ExcludeDocsQuery(seenDocs, searcher.getIndexReader()),
                BooleanClause.Occur.FILTER
            );
        }
        booleanQueryBuilder = booleanQueryBuilder.add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER);
        Query rewritten = searcher.rewrite(booleanQueryBuilder.build());
        if (rewritten.getClass() == MatchNoDocsQuery.class) {
            return null;
        }
        return searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1f);
    }
}
