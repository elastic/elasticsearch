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
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.function.LongConsumer;

/**
 * Shared logic for post-filtering in HNSW knn queries ({@link ESKnnFloatVectorQuery},
 * {@link ESKnnByteVectorQuery}). These classes extend different Lucene base classes
 * and cannot share a Java abstract base, so common logic lives here as static methods.
 */
final class KnnPostFilterHelper {

    private KnnPostFilterHelper() {}

    /**
     * Checks if post-filtering should be used and returns the post-filter rewrite, or null
     * if the standard pre-filter path should be taken.
     */
    static Query maybePostFilterRewrite(
        IndexSearcher indexSearcher,
        Query filter,
        String field,
        boolean skipPostFilter,
        VectorCountSupplier vectorCountSupplier,
        PostFilterDelegateFactory delegateFactory,
        int kParam,
        KnnSearchStrategy searchStrategy,
        boolean earlyTermination,
        LongConsumer vectorOpsCallback,
        BitSetProducer parentsFilter
    ) throws IOException {
        if (skipPostFilter || filter == null) {
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
            searchStrategy,
            earlyTermination,
            delegateFactory,
            vectorOpsCallback,
            parentsFilter
        );
    }

    private static float computeSelectivity(
        Weight filterWeight,
        IndexSearcher indexSearcher,
        String field,
        VectorCountSupplier vectorCountSupplier
    ) throws IOException {
        long totalVectors = 0;
        long filterCost = 0;
        for (LeafReaderContext leafCtx : indexSearcher.getIndexReader().leaves()) {
            totalVectors += vectorCountSupplier.countVectors(leafCtx);
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
        KnnSearchStrategy searchStrategy,
        boolean earlyTermination,
        PostFilterDelegateFactory delegateFactory,
        LongConsumer vectorOpsCallback,
        BitSetProducer parentsFilter
    ) throws IOException {
        int scaledNumCands = (int) Math.ceil(kParam / selectivity);
        KnnSearchStrategy seeded = new KnnSearchStrategy.Seeded(null, 0, searchStrategy);
        PostFilterableKnnQuery delegate = delegateFactory.create(scaledNumCands, seeded, earlyTermination);
        IndexReader reader = searcher.getIndexReader();
        return new PostFilterAwareKnnQuery(delegate, filterWeight, kParam, reader, vectorOpsCallback, parentsFilter);
    }

    /**
     * Builds the retry query state: computes the new seenDocs bitset from previous seenDocs
     * and the current round's results.
     */
    static FixedBitSet buildRetrySeenDocs(FixedBitSet previousSeenDocs, TopDocs capturedResults, IndexReader reader) {
        int maxDoc = reader.maxDoc();
        FixedBitSet newSeenDocs = new FixedBitSet(Math.max(maxDoc, 1));
        if (previousSeenDocs != null) {
            newSeenDocs.or(previousSeenDocs);
        }
        if (capturedResults != null) {
            for (ScoreDoc sd : capturedResults.scoreDocs) {
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
    static KnnCollectorManager wrapCollectorManager(KnnCollectorManager base, TopDocs seedResults, String field, boolean earlyTermination) {
        if (seedResults != null && seedResults.scoreDocs.length > 0) {
            base = new SeededRetryCollectorManager(base, seedResults, field);
        }
        return earlyTermination ? PatienceCollectorManager.wrap(base) : base;
    }

    /** Counts vectors in a leaf — implemented differently for float vs byte. */
    @FunctionalInterface
    interface VectorCountSupplier {
        long countVectors(LeafReaderContext ctx) throws IOException;
    }

    /** Creates a filter-less delegate for post-filtering — type-specific. */
    @FunctionalInterface
    interface PostFilterDelegateFactory {
        PostFilterableKnnQuery create(int scaledNumCands, KnnSearchStrategy strategy, boolean earlyTermination);
    }
}
