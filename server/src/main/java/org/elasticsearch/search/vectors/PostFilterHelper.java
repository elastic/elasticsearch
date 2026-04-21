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
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.function.LongConsumer;

/**
 * Static helper methods for post-filtering logic shared by HNSW and IVF query implementations.
 * Extracted from {@link PostFilterableKnnQuery} default methods for testability and clarity.
 */
final class PostFilterHelper {

    private PostFilterHelper() {}

    /**
     * Determines whether post-filtering should be applied and, if so, creates the
     * {@link PostFilterAwareKnnQuery} wrapper. Returns null if post-filtering should not apply
     * (delegate is already post-filtering, no filter, filter matches no docs, or selectivity
     * is below the threshold).
     */
    static Query maybePostFilterRewrite(
        IndexSearcher indexSearcher,
        Query filter,
        String field,
        PostFilterableKnnQuery.VectorCountSupplier vectorCountSupplier,
        PostFilterableKnnQuery.PostFilterDelegateFactory delegateFactory,
        int kParam,
        int numCands,
        KnnSearchStrategy searchStrategy,
        boolean earlyTermination,
        LongConsumer vectorOpsCallback,
        BitSetProducer parentsFilter
    ) throws IOException {
        if (filter == null) {
            return null;
        }
        BooleanQuery booleanQuery = new BooleanQuery.Builder().add(filter, BooleanClause.Occur.FILTER)
            .add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER)
            .build();
        Query rewritten = indexSearcher.rewrite(booleanQuery);
        if (rewritten.getClass() == MatchNoDocsQuery.class) {
            return null;
        }
        Weight filterWeight = indexSearcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1f);
        float selectivity = computeSelectivity(filterWeight, indexSearcher, field, vectorCountSupplier);
        if (selectivity <= PostFilterAwareKnnQuery.POST_FILTERING_THRESHOLD) {
            return null;
        }
        return postFilterRewrite(
            indexSearcher,
            filterWeight,
            selectivity,
            kParam,
            numCands,
            searchStrategy,
            earlyTermination,
            delegateFactory,
            vectorOpsCallback,
            parentsFilter
        );
    }

    /**
     * Computes filter selectivity as the ratio of filter-matching docs to total vectors.
     * Returns 0 if there are no vectors.
     */
    static float computeSelectivity(
        Weight filterWeight,
        IndexSearcher indexSearcher,
        String field,
        PostFilterableKnnQuery.VectorCountSupplier vectorCountSupplier
    ) throws IOException {
        long totalVectors = 0;
        long filterCost = 0;
        for (LeafReaderContext leafCtx : indexSearcher.getIndexReader().leaves()) {
            totalVectors += vectorCountSupplier.totalVectors(leafCtx);
            ScorerSupplier ss = filterWeight.scorerSupplier(leafCtx);
            if (ss != null) {
                filterCost += ss.cost();
            }
        }
        return totalVectors > 0 ? Math.min(1f, (float) filterCost / totalVectors) : 0f;
    }

    private static Query postFilterRewrite(
        IndexSearcher searcher,
        Weight filterWeight,
        float selectivity,
        int kParam,
        int numCands,
        KnnSearchStrategy searchStrategy,
        boolean earlyTermination,
        PostFilterableKnnQuery.PostFilterDelegateFactory delegateFactory,
        LongConsumer vectorOpsCallback,
        BitSetProducer parentsFilter
    ) throws IOException {
        int scaledK = (int) Math.ceil(kParam / selectivity);
        int scaledNumCands = (int) Math.min(Integer.MAX_VALUE, Math.ceil((double) numCands / selectivity));
        PostFilterableKnnQuery delegate = delegateFactory.create(scaledK, scaledNumCands, searchStrategy, earlyTermination);
        IndexReader reader = searcher.getIndexReader();
        return new PostFilterAwareKnnQuery(delegate, filterWeight, kParam, reader, vectorOpsCallback, parentsFilter);
    }

    /**
     * Builds the cumulative seenDocs bitset from previous rounds' results, used by HNSW retry
     * to avoid re-visiting documents.
     */
    static FixedBitSet buildRetrySeenDocs(FixedBitSet previousSeenDocs, ScoreDoc[] previousResults, IndexReader reader) {
        int maxDoc = reader.maxDoc();
        FixedBitSet newSeenDocs = new FixedBitSet(Math.max(maxDoc, 1));
        if (previousSeenDocs != null) {
            newSeenDocs.or(previousSeenDocs);
        }
        if (previousResults != null) {
            for (ScoreDoc sd : previousResults) {
                if (sd.doc >= 0 && sd.doc < maxDoc) {
                    newSeenDocs.set(sd.doc);
                }
            }
        }
        return newSeenDocs;
    }

    /**
     * Wraps the base collector manager with seeded retry and patience if appropriate.
     */
    static KnnCollectorManager wrapCollectorManager(
        KnnCollectorManager base,
        ScoreDoc[] seedResults,
        String field,
        boolean earlyTermination
    ) {
        if (seedResults != null && seedResults.length > 0) {
            base = new SeededRetryCollectorManager(base, seedResults, field);
        }
        return earlyTermination ? PatienceCollectorManager.wrap(base) : base;
    }
}
