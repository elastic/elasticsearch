/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class SignificanceLookupTests extends ESTestCase {

    private static final int NUM_DOCS = 5_000;

    private Directory dir;
    private IndexReader reader;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dir = newDirectory();
        try (RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            // Pin the segment layout so per-leaf assertions are deterministic.
            w.w.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
            w.setDoRandomForceMerge(false);
            for (int i = 0; i < NUM_DOCS; i++) {
                Document doc = new Document();
                doc.add(new StringField("k", (i % 2 == 0) ? "a" : "b", Field.Store.NO));
                w.addDocument(doc);
            }
            reader = w.getReader();
        }
    }

    @Override
    public void tearDown() throws Exception {
        IOUtils.close(reader, dir);
        super.tearDown();
    }

    /**
     * Wraps {@code inner} so that its {@link Weight#count(LeafReaderContext)} returns -1, forcing
     * Lucene's count machinery to fall through to {@link IndexSearcher#searchLeaf} and ultimately
     * call {@code scorerSupplier(ctx).bulkScorer()} — which is the path the cancellable wrapper
     * intercepts. This mimics what real script-field queries do (their {@code ConstantScoreWeight}
     * does not override {@code count}, so it inherits the default {@code -1}).
     */
    private static Query forceBulkScoring(Query inner) {
        return new Query() {
            @Override
            public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
                Weight delegate = inner.createWeight(searcher, scoreMode, boost);
                return new FilterWeight(this, delegate) {
                    @Override
                    public int count(LeafReaderContext ctx) {
                        return -1;
                    }
                };
            }

            @Override
            public Query rewrite(IndexSearcher searcher) throws IOException {
                Query rewritten = inner.rewrite(searcher);
                return rewritten == inner ? this : forceBulkScoring(rewritten);
            }

            @Override
            public String toString(String field) {
                return "forceBulkScoring(" + inner.toString(field) + ")";
            }

            @Override
            public void visit(QueryVisitor visitor) {
                inner.visit(visitor);
            }

            @Override
            public boolean equals(Object o) {
                return o == this;
            }

            @Override
            public int hashCode() {
                return System.identityHashCode(this);
            }
        };
    }

    /**
     * Counts must agree with a vanilla {@link IndexSearcher} for queries that hit the fast count
     * path (constant-time {@code Weight.count}) — these never reach the bulk-scoring loop, so
     * the only thing the wrapper contributes here is the per-leaf cancellation check.
     */
    public void testCountMatchesIndexSearcherForFastCountQueries() throws IOException {
        IndexSearcher cancellable = SignificanceLookup.cancellableSearcher(reader, () -> {});
        IndexSearcher reference = new IndexSearcher(reader);

        Query matchAll = new MatchAllDocsQuery();
        assertThat(cancellable.count(matchAll), equalTo(reference.count(matchAll)));
        assertThat(cancellable.count(matchAll), equalTo(NUM_DOCS));

        Query termA = new TermQuery(new Term("k", "a"));
        assertThat(cancellable.count(termA), equalTo(reference.count(termA)));
        assertThat(cancellable.count(termA), equalTo(NUM_DOCS / 2));
    }

    /**
     * Counts must also agree with a vanilla {@link IndexSearcher} when the query is forced down
     * the bulk-scoring path. This is the regression check for #144442 — without correct wiring
     * of the {@link FilterWeight}/{@code ScorerSupplier}/{@code CancellableBulkScorer} chain,
     * counts could either drop matches or double-count them.
     */
    public void testCountMatchesIndexSearcherForBulkScoringQueries() throws IOException {
        IndexSearcher cancellable = SignificanceLookup.cancellableSearcher(reader, () -> {});
        IndexSearcher reference = new IndexSearcher(reader);

        Query bulkScoredAll = forceBulkScoring(new MatchAllDocsQuery());
        assertThat(cancellable.count(bulkScoredAll), equalTo(reference.count(bulkScoredAll)));
        assertThat(cancellable.count(bulkScoredAll), equalTo(NUM_DOCS));

        Query bulkScoredTerm = forceBulkScoring(new TermQuery(new Term("k", "a")));
        assertThat(cancellable.count(bulkScoredTerm), equalTo(reference.count(bulkScoredTerm)));
        assertThat(cancellable.count(bulkScoredTerm), equalTo(NUM_DOCS / 2));
    }

    /**
     * If the task is already cancelled before counting starts, the searcher fails fast with a
     * {@link TaskCancelledException} from the per-leaf cancellation check installed in {@code
     * searchLeaf}, regardless of whether the query takes the fast count or bulk-scoring path.
     */
    public void testCountThrowsTaskCancelledWhenCancelledUpFront() {
        Runnable alwaysCancelled = () -> { throw new TaskCancelledException("cancelled"); };
        IndexSearcher cancellable = SignificanceLookup.cancellableSearcher(reader, alwaysCancelled);
        expectThrows(TaskCancelledException.class, () -> cancellable.count(new MatchAllDocsQuery()));
        expectThrows(TaskCancelledException.class, () -> cancellable.count(forceBulkScoring(new MatchAllDocsQuery())));
    }

    /**
     * Regression check for #144442: when a query goes through the bulk-scoring loop, the
     * cancellation runnable must be invoked from inside that loop — not just at the start of
     * each leaf. Without the {@link org.elasticsearch.search.internal.CancellableBulkScorer}
     * wrap on {@code ScorerSupplier.bulkScorer()}, a cancellation arriving while the scorer was
     * iterating would never be observed.
     */
    public void testCancellationRunnableIsInvokedDuringBulkScoring() throws IOException {
        AtomicInteger checks = new AtomicInteger(0);
        IndexSearcher cancellable = SignificanceLookup.cancellableSearcher(reader, checks::incrementAndGet);

        cancellable.count(forceBulkScoring(new MatchAllDocsQuery()));

        int leaves = reader.leaves().size();
        // Per leaf the wrapper invokes the runnable once before super.searchLeaf, plus at least
        // the entry and exit checks of CancellableBulkScorer inside the bulk-scoring loop.
        // Asserting strictly greater than `leaves` proves the bulk-scorer wrap is reached.
        assertThat("checkCancelled should be invoked from the bulk-scoring path", checks.get(), greaterThan(leaves));
    }

    /**
     * If the cancellation flag flips while bulk scoring is in progress, the next batch boundary
     * inside {@link org.elasticsearch.search.internal.CancellableBulkScorer} must surface the
     * exception rather than letting the count run to completion.
     */
    public void testCountAbortsWhenCancellationFlipsDuringScoring() {
        AtomicBoolean cancelled = new AtomicBoolean(false);
        Runnable checkCancelled = () -> {
            if (cancelled.get()) {
                throw new TaskCancelledException("cancelled");
            }
            // Flip after the first observation so the very next invocation throws — this proves
            // a cancellation that lands mid-scoring is observed by CancellableBulkScorer's
            // per-batch check, not just by the per-leaf check before super.searchLeaf.
            cancelled.set(true);
        };

        IndexSearcher cancellable = SignificanceLookup.cancellableSearcher(reader, checkCancelled);
        expectThrows(TaskCancelledException.class, () -> cancellable.count(forceBulkScoring(new MatchAllDocsQuery())));
    }
}
