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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
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
     * The cancellable searcher must return identical counts to a vanilla {@link IndexSearcher},
     * both for queries that hit the constant-time count path ({@link MatchAllDocsQuery}) and for
     * queries that go through scoring ({@link BooleanQuery} with two MUST clauses).
     */
    public void testCountMatchesIndexSearcherWhenNotCancelled() throws IOException {
        IndexSearcher cancellable = SignificanceLookup.cancellableSearcher(reader, () -> {});
        IndexSearcher reference = new IndexSearcher(reader);

        Query matchAll = new MatchAllDocsQuery();
        assertThat(cancellable.count(matchAll), equalTo(reference.count(matchAll)));
        assertThat(cancellable.count(matchAll), equalTo(NUM_DOCS));

        Query termA = new TermQuery(new Term("k", "a"));
        assertThat(cancellable.count(termA), equalTo(reference.count(termA)));
        assertThat(cancellable.count(termA), equalTo(NUM_DOCS / 2));

        // BooleanQuery with two MUST clauses defeats the fast-count optimization, exercising the
        // bulk-scoring path through the cancellable wrapper.
        Query both = new BooleanQuery.Builder().add(new TermQuery(new Term("k", "a")), BooleanClause.Occur.MUST)
            .add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
            .build();
        assertThat(cancellable.count(both), equalTo(reference.count(both)));
    }

    /**
     * If the task is already cancelled before counting starts, the searcher fails fast with a
     * {@link TaskCancelledException} from the per-leaf cancellation check installed in {@code
     * searchLeaf}.
     */
    public void testCountThrowsTaskCancelledWhenCancelledUpFront() {
        Runnable alwaysCancelled = () -> { throw new TaskCancelledException("cancelled"); };
        IndexSearcher cancellable = SignificanceLookup.cancellableSearcher(reader, alwaysCancelled);
        expectThrows(TaskCancelledException.class, () -> cancellable.count(new MatchAllDocsQuery()));
        expectThrows(TaskCancelledException.class, () -> cancellable.count(new TermQuery(new Term("k", "a"))));
    }

    /**
     * Regression check for #144442: counting a query that scores per-doc must invoke the
     * cancellation runnable while scoring, not only at the start of each leaf — otherwise a
     * cancellation that arrives during scoring of a single batch would never be observed.
     * The {@link org.elasticsearch.search.internal.CancellableBulkScorer} wrap installed on the
     * inner {@link org.apache.lucene.search.ScorerSupplier} provides those mid-scoring checks.
     */
    public void testCancellationRunnableIsInvokedDuringBulkScoring() throws IOException {
        AtomicInteger checks = new AtomicInteger(0);
        IndexSearcher cancellable = SignificanceLookup.cancellableSearcher(reader, checks::incrementAndGet);

        // BooleanQuery with two MUST clauses goes through the bulk-scoring loop on every leaf,
        // so the CancellableBulkScorer's per-batch checks fire in addition to the per-leaf check.
        Query scored = new BooleanQuery.Builder().add(new TermQuery(new Term("k", "a")), BooleanClause.Occur.MUST)
            .add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
            .build();
        cancellable.count(scored);

        int leaves = reader.leaves().size();
        // At minimum we expect the per-leaf check before super.searchLeaf, plus the entry and
        // exit checks of CancellableBulkScorer for each leaf that has a non-null bulk scorer.
        assertThat("checkCancelled should be invoked from the bulk-scoring path", checks.get(), greaterThan(leaves));
    }

    /**
     * If the cancellation flag flips while scoring is in progress, the next batch boundary
     * inside {@link org.elasticsearch.search.internal.CancellableBulkScorer} must surface the
     * exception rather than letting the count run to completion.
     */
    public void testCountAbortsWhenCancellationFlipsDuringScoring() {
        AtomicInteger checks = new AtomicInteger(0);
        AtomicBoolean cancelled = new AtomicBoolean(false);
        Runnable checkCancelled = () -> {
            // Flip on the first invocation; the searcher's first action on each leaf is to call
            // this runnable, so the very next leaf will throw.
            checks.incrementAndGet();
            if (cancelled.get()) {
                throw new TaskCancelledException("cancelled");
            }
            cancelled.set(true);
        };

        IndexSearcher cancellable = SignificanceLookup.cancellableSearcher(reader, checkCancelled);
        Query scored = new BooleanQuery.Builder().add(new TermQuery(new Term("k", "a")), BooleanClause.Occur.MUST)
            .add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
            .build();
        expectThrows(TaskCancelledException.class, () -> cancellable.count(scored));
    }
}
