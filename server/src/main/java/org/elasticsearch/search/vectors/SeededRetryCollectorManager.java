/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;

import java.io.IOException;
import java.util.Arrays;

/**
 * A {@link KnnCollectorManager} decorator that seeds the HNSW search with entry points
 * from a previous round's results. Per-leaf, it maps the global doc IDs to vector ordinals
 * and wraps the search strategy with {@link KnnSearchStrategy.Seeded}.
 */
class SeededRetryCollectorManager implements KnnCollectorManager {

    private final KnnCollectorManager delegate;
    private final int[][] seedDocsPerLeaf;
    private final String field;

    SeededRetryCollectorManager(KnnCollectorManager delegate, int[][] seedDocsPerLeaf, String field) {
        this.delegate = delegate;
        this.seedDocsPerLeaf = seedDocsPerLeaf;
        this.field = field;
    }

    @Override
    public KnnCollector newCollector(int visitLimit, KnnSearchStrategy searchStrategy, LeafReaderContext ctx) throws IOException {
        SeedResult seeds = buildSeedOrdinals(ctx);
        if (seeds == null) {
            return delegate.newCollector(visitLimit, searchStrategy, ctx);
        }
        KnnSearchStrategy seeded = new KnnSearchStrategy.Seeded(seeds.ordinals, seeds.count, searchStrategy);
        return delegate.newCollector(visitLimit, seeded, ctx);
    }

    @Override
    public KnnCollector newOptimisticCollector(int visitLimit, KnnSearchStrategy searchStrategy, LeafReaderContext ctx, int k)
        throws IOException {
        if (delegate.isOptimistic()) {
            SeedResult seeds = buildSeedOrdinals(ctx);
            if (seeds != null) {
                searchStrategy = new KnnSearchStrategy.Seeded(seeds.ordinals, seeds.count, searchStrategy);
            }
            return delegate.newOptimisticCollector(visitLimit, searchStrategy, ctx, k);
        }
        return null;
    }

    @Override
    public boolean isOptimistic() {
        return delegate.isOptimistic();
    }

    private record SeedResult(DocIdSetIterator ordinals, int count) {}

    /**
     * Maps this leaf's global seed doc IDs to vector ordinals. The seeds are already capped and
     * proximity-selected per leaf upstream (see PostFilterKnnQuery#nearestSeedsPerLeaf) and indexed by
     * leaf ordinal, so we look them up directly by {@code ctx.ord}. Returns null if this leaf has no
     * seeds or if vector values are unavailable.
     */
    private SeedResult buildSeedOrdinals(LeafReaderContext ctx) throws IOException {
        int[] leafSeeds = ctx.ord < seedDocsPerLeaf.length ? seedDocsPerLeaf[ctx.ord] : null;
        if (leafSeeds == null || leafSeeds.length == 0) {
            return null;
        }
        int docBase = ctx.docBase;

        // Map doc IDs to vector ordinals via the vector values iterator
        KnnVectorValues.DocIndexIterator docIndexIter = getDocIndexIterator(ctx);
        if (docIndexIter == null) {
            return null;
        }
        int[] ordinals = new int[leafSeeds.length];
        int ordCount = 0;
        int iterDoc = -1;
        for (int seedDoc : leafSeeds) {
            int docId = seedDoc - docBase;
            if (docId <= iterDoc) {
                continue;
            }
            iterDoc = docIndexIter.advance(docId);
            if (iterDoc == docId) {
                ordinals[ordCount++] = docIndexIter.index();
            }
        }
        if (ordCount == 0) {
            return null;
        }

        final int finalCount = ordCount;
        final int[] finalOrdinals = Arrays.copyOf(ordinals, ordCount);
        DocIdSetIterator disi = new DocIdSetIterator() {
            int idx = -1;

            @Override
            public int docID() {
                if (idx < 0) return -1;
                if (idx >= finalCount) return NO_MORE_DOCS;
                return finalOrdinals[idx];
            }

            @Override
            public int nextDoc() {
                idx++;
                return docID();
            }

            @Override
            public int advance(int target) {
                if (idx >= finalCount) {
                    return NO_MORE_DOCS;
                }
                int pos = Arrays.binarySearch(finalOrdinals, Math.max(0, idx), finalCount, target);
                idx = pos >= 0 ? pos : -pos - 1;
                return docID();
            }

            @Override
            public long cost() {
                return finalCount;
            }
        };
        return new SeedResult(disi, finalCount);
    }

    private KnnVectorValues.DocIndexIterator getDocIndexIterator(LeafReaderContext ctx) throws IOException {
        FloatVectorValues fvv = ctx.reader().getFloatVectorValues(field);
        if (fvv != null) {
            return fvv.iterator();
        }
        ByteVectorValues bvv = ctx.reader().getByteVectorValues(field);
        if (bvv != null) {
            return bvv.iterator();
        }
        return null;
    }
}
