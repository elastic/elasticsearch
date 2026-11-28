/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.elasticsearch.lucene.search.cost.FixedBitSetCost;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.LongConsumer;

/**
 * Transparent {@link Weight} wrapper that charges the request circuit breaker for the per-leaf execution RAM a query
 * allocates, sized by the {@link ExecutionCharge}'s estimator when a scorer is built and released by the
 * {@link LeafExecutionScope} that built it, or by {@link ContextIndexSearcher#searchLeaf} when there is none. A leaf the
 * estimator sizes at {@code 0} is not charged. Charges go through {@link ContextIndexSearcher#reserveLeaf} and
 * {@link ContextIndexSearcher#chargeLeaf} rather than a cached accounting reference, so this survives a
 * {@link ContextIndexSearcher#setCircuitBreaker} swap. See {@link ExecutionCharge#forQuery} for the queries that are charged.
 * <p>
 * The estimate is a reservation for the most the build may hold at once. Once {@link ScorerSupplier#get} returns, the
 * reservation is settled down to the doc id set the scorer keeps, when {@link #retainedBytes} can tell what that is.
 * {@link ScorerSupplier#bulkScorer} does not expose the scorer, so a bulk scorer keeps its reservation until it is released.
 */
final class ExecutionBreakerWeight extends FilterWeight {

    /** The class of the doc id range iterator behind {@link DocIdSetIterator#all} and {@link DocIdSetIterator#empty}, private to Lucene. */
    static final Class<? extends DocIdSetIterator> DOC_ID_RANGE_CLASS = DocIdSetIterator.all(1).getClass();

    /** The class of the iterator over the sorted {@code int[]} that a sparse {@link DocIdSetBuilder} builds, private to Lucene. */
    static final Class<? extends DocIdSetIterator> INT_ARRAY_ITERATOR_CLASS = intArrayIteratorClass();

    /**
     * The iterator over a {@code DocIdSetBuilder}'s sorted {@code int[]} and the header of that array.
     * Conservative without compressed oops.
     */
    static final long INT_ARRAY_ITERATOR_BYTES = 64L;

    private final ContextIndexSearcher searcher;
    private final ExecutionCharge charge;

    ExecutionBreakerWeight(ContextIndexSearcher searcher, Weight in, ExecutionCharge charge) {
        super(in);
        this.searcher = searcher;
        this.charge = charge;
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final ScorerSupplier scorerSupplier = in.scorerSupplier(context);
        if (scorerSupplier == null) {
            return null;
        }

        return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) throws IOException {
                final LongConsumer settle = searcher.reserveLeaf(context, bytesForLeaf(context, scorerSupplier, leadCost), charge.label());
                final Scorer scorer = scorerSupplier.get(leadCost);
                if (settle != null) {
                    settle.accept(retainedBytes(scorer.iterator(), context.reader().maxDoc()));
                }
                return scorer;
            }

            @Override
            public BulkScorer bulkScorer() throws IOException {
                searcher.chargeLeaf(context, bytesForLeaf(context, scorerSupplier, Long.MAX_VALUE), charge.label());
                return scorerSupplier.bulkScorer();
            }

            @Override
            public long cost() {
                return scorerSupplier.cost();
            }

            @Override
            public void setTopLevelScoringClause() throws IOException {
                scorerSupplier.setTopLevelScoringClause();
            }
        };
    }

    private long bytesForLeaf(LeafReaderContext context, ScorerSupplier inner, long leadCost) throws IOException {
        return charge.estimator().bytesForLeaf(context, inner, leadCost);
    }

    /**
     * Ceiling on the heap of the doc id set that {@code iterator}, a freshly built scorer's iterator on a leaf of {@code maxDoc}
     * documents, keeps reachable, or {@link Long#MAX_VALUE} when that cannot be told, which keeps the whole reservation:
     * <ul>
     *   <li>a doc id range keeps nothing;</li>
     *   <li>a {@link BitSetIterator} keeps its bit set;</li>
     *   <li>the iterator of a sparse {@link DocIdSetBuilder} keeps the builder's largest buffer, which can be much longer than
     *       {@link DocIdSetIterator#cost()}. The builder never allocates more than {@code maxDoc >>> 7} entries in total, and
     *       {@code build()} grows the array to one more than the entries it holds at most.</li>
     * </ul>
     * Anything else, including the wrapper {@code ConstantScoreScorer} puts around its iterator for {@code TOP_SCORES} and
     * two-phase or disjunction iterators, may hold more than a doc id set.
     */
    static long retainedBytes(DocIdSetIterator iterator, int maxDoc) {
        if (iterator.getClass() == DOC_ID_RANGE_CLASS) {
            return 0L;
        }
        if (iterator.getClass() == BitSetIterator.class) {
            return ((BitSetIterator) iterator).getBitSet().ramBytesUsed() + FixedBitSetCost.ITERATOR_BYTES;
        }
        if (iterator.getClass() == INT_ARRAY_ITERATOR_CLASS) {
            return INT_ARRAY_ITERATOR_BYTES + Integer.BYTES * ((long) (maxDoc >>> 7) + 1);
        }
        return Long.MAX_VALUE;
    }

    private static Class<? extends DocIdSetIterator> intArrayIteratorClass() {
        final DocIdSetBuilder builder = new DocIdSetBuilder(1 << 7);
        builder.grow(1).add(0);
        try {
            return builder.build().iterator().getClass();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
