/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link Query} wrapper that delegates all Lucene scoring to an inner query and exposes
 * the field name and numeric range bounds. When the underlying doc values implement
 * {@link BlockLoader.OptionalBulkNumericFilter}, scoring decodes values block-at-a-time,
 * using a {@link DocValuesSkipper} (when available) to skip blocks whose min/max excludes
 * the range entirely.
 */
public final class EsNumericRangeQuery extends Query {

    /**
     * Number of docs processed per batch. Matches the default TSDB codec block size (2^7 = 128)
     * so that each batch maps to exactly one codec block and is decoded at most once.
     */
    private static final int BULK_FILTER_BATCH_SIZE = 128;

    private final Query delegate;
    private final String field;
    private final long lowerValue;
    private final long upperValue;

    public EsNumericRangeQuery(Query delegate, String field, long lowerValue, long upperValue) {
        this.delegate = Objects.requireNonNull(delegate);
        this.field = Objects.requireNonNull(field);
        this.lowerValue = lowerValue;
        this.upperValue = upperValue;
    }

    public String field() {
        return field;
    }

    public long lowerValue() {
        return lowerValue;
    }

    public long upperValue() {
        return upperValue;
    }

    public Query getDelegate() {
        return delegate;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new EsNumericRangeWeight(delegate.createWeight(searcher, scoreMode, boost), scoreMode);
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Query rewritten = delegate.rewrite(searcher);
        if (rewritten == delegate) {
            return super.rewrite(searcher);
        }
        return new EsNumericRangeQuery(rewritten, field, lowerValue, upperValue);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        delegate.visit(visitor);
    }

    @Override
    public String toString(String fieldName) {
        return "EsNumericRangeQuery(" + delegate.toString(fieldName) + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (sameClassAs(o) == false) return false;
        EsNumericRangeQuery that = (EsNumericRangeQuery) o;
        return lowerValue == that.lowerValue
            && upperValue == that.upperValue
            && Objects.equals(field, that.field)
            && Objects.equals(delegate, that.delegate);
    }

    @Override
    public int hashCode() {
        return classHash() ^ Objects.hash(delegate, field, lowerValue, upperValue);
    }

    /**
     * Returns a {@link DocIdSetIterator} that evaluates the range predicate block-at-a-time.
     * <p>
     * For each batch, the optional {@link DocValuesSkipper} is consulted first: blocks whose
     * min/max range excludes {@code [lower, upper]} at any skipper level are skipped without
     * decoding. Candidate blocks are decoded once and matching doc IDs collected directly into
     * {@code pending[]} via {@link BlockLoader.OptionalBulkNumericFilter#tryCollectMatchingDocs}
     * in a single pass — no separate mask-scan step.
     */
    private static DocIdSetIterator createBulkRangeIterator(
        BlockLoader.OptionalBulkNumericFilter filter,
        int maxDoc,
        long lower,
        long upper,
        Bits liveDocs,
        DocValuesSkipper skipper
    ) {
        return new DocIdSetIterator() {
            private int currentDoc = -1;
            private int nextBatchStart = 0;
            private int pendingIdx = 0;
            private int pendingCount = 0;
            private final int[] pending = new int[BULK_FILTER_BATCH_SIZE];

            @Override
            public int docID() {
                return currentDoc;
            }

            /**
             * Advances {@code target} past any blocks that the skipper can rule out at any level,
             * returning the first batch-aligned start that might contain matching docs,
             * or {@code maxDoc} if exhausted.
             */
            private int skipToCandidate(int target) throws IOException {
                while (target < maxDoc) {
                    skipper.advance(target);
                    int minDoc = skipper.minDocID(0);
                    if (minDoc == NO_MORE_DOCS) {
                        return maxDoc;
                    }
                    target = Math.max(target, minDoc);

                    // Check from the coarsest level down; the first level that excludes the range
                    // tells us how far we can skip in one jump.
                    boolean skipped = false;
                    for (int level = skipper.numLevels() - 1; level >= 0; level--) {
                        if (skipper.minValue(level) > upper || skipper.maxValue(level) < lower) {
                            target = skipper.maxDocID(level) + 1;
                            skipped = true;
                            break;
                        }
                    }
                    if (skipped == false) {
                        // Block is a candidate — align to batch boundary and return
                        return (target / BULK_FILTER_BATCH_SIZE) * BULK_FILTER_BATCH_SIZE;
                    }
                }
                return maxDoc;
            }

            @Override
            public int nextDoc() throws IOException {
                while (pendingIdx >= pendingCount) {
                    if (nextBatchStart >= maxDoc) {
                        return currentDoc = NO_MORE_DOCS;
                    }
                    if (skipper != null) {
                        nextBatchStart = skipToCandidate(nextBatchStart);
                        if (nextBatchStart >= maxDoc) {
                            return currentDoc = NO_MORE_DOCS;
                        }
                    }
                    int batchStart = nextBatchStart;
                    int batchSize = Math.min(BULK_FILTER_BATCH_SIZE, maxDoc - batchStart);
                    nextBatchStart = batchStart + batchSize;
                    int n = filter.tryCollectMatchingDocs(batchStart, batchSize, lower, upper, pending);
                    if (liveDocs == null) {
                        pendingCount = n;
                    } else {
                        pendingCount = 0;
                        for (int i = 0; i < n; i++) {
                            if (liveDocs.get(pending[i])) {
                                pending[pendingCount++] = pending[i];
                            }
                        }
                    }
                    pendingIdx = 0;
                }
                return currentDoc = pending[pendingIdx++];
            }

            @Override
            public int advance(int target) throws IOException {
                int targetBatchStart = (target / BULK_FILTER_BATCH_SIZE) * BULK_FILTER_BATCH_SIZE;
                if (targetBatchStart >= nextBatchStart) {
                    nextBatchStart = targetBatchStart;
                    pendingIdx = pendingCount = 0;
                }
                int d;
                do {
                    d = nextDoc();
                } while (d < target && d != NO_MORE_DOCS);
                return d;
            }

            @Override
            public long cost() {
                return skipper != null ? skipper.docCount() : maxDoc;
            }
        };
    }

    /**
     * Wraps an inner {@link Weight} so that {@link #getQuery()} returns the enclosing
     * {@link EsNumericRangeQuery} rather than the delegate query. When the field's doc
     * values implement {@link BlockLoader.OptionalBulkNumericFilter}, scoring uses the
     * block-level iterator (with optional skipper) instead of the delegate scorer.
     */
    private class EsNumericRangeWeight extends Weight {
        private final Weight inner;
        private final ScoreMode scoreMode;

        EsNumericRangeWeight(Weight inner, ScoreMode scoreMode) {
            super(EsNumericRangeQuery.this);
            this.inner = inner;
            this.scoreMode = scoreMode;
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            SortedNumericDocValues dv = context.reader().getSortedNumericDocValues(field);
            if (dv instanceof BlockLoader.OptionalBulkNumericFilter bulkFilter) {
                final int maxDoc = context.reader().maxDoc();
                final Bits liveDocs = context.reader().getLiveDocs();
                final DocValuesSkipper skipper = context.reader().getDocValuesSkipper(field);
                return new ScorerSupplier() {
                    @Override
                    public Scorer get(long leadCost) {
                        return new ConstantScoreScorer(
                            0f,
                            scoreMode,
                            createBulkRangeIterator(bulkFilter, maxDoc, lowerValue, upperValue, liveDocs, skipper)
                        );
                    }

                    @Override
                    public long cost() {
                        return skipper != null ? skipper.docCount() : maxDoc;
                    }
                };
            }
            return inner.scorerSupplier(context);
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return inner.explain(context, doc);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return inner.isCacheable(ctx);
        }

        @Override
        public int count(LeafReaderContext context) throws IOException {
            return inner.count(context);
        }
    }
}
