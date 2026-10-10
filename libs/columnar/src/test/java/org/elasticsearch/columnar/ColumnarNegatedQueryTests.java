/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.ConstantScoreScorerSupplier;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;

/**
 * {@link ColumnarNegatedQuery} on its own terms. What it must agree with over real columns is covered by
 * {@link ColumnarStringQueryConsistencyTests}; here is how it is chosen, how it rewrites, and the property that justifies
 * it: the exclusion is confirmed only on the documents the other clauses kept.
 */
public class ColumnarNegatedQueryTests extends ESTestCase {

    private static final ScanBudget NO_BUDGET = s -> {};

    public void testAsFilterAcceptsAnExclusionThatScansAColumn() {
        final Query term = ColumnarStringTermQuery.term("f", new BytesRef("x"), NO_BUDGET);
        assertEquals(new ColumnarNegatedQuery(term), ColumnarNegatedQuery.asFilter(term));
        assertNotNull(ColumnarNegatedQuery.asFilter(ColumnarStringTermQuery.contains("f", new BytesRef("x"), NO_BUDGET)));
        assertNotNull(ColumnarNegatedQuery.asFilter(ColumnarStringTermQuery.prefix("f", new BytesRef("x"), NO_BUDGET)));
        assertNotNull(
            ColumnarNegatedQuery.asFilter(new ColumnarStringRangeQuery("f", new BytesRef("a"), true, new BytesRef("z"), true, NO_BUDGET))
        );
        assertNotNull(
            ColumnarNegatedQuery.asFilter(new ColumnarStringAnyOfQuery("f", List.of(new BytesRef("a"), new BytesRef("b")), NO_BUDGET))
        );
    }

    public void testAsFilterSeesThroughWrappers() {
        // ES|QL wraps what it pushes down in single-value checks and filters, so the scan is rarely the clause itself.
        final Query term = ColumnarStringTermQuery.contains("f", new BytesRef("x"), NO_BUDGET);
        assertNotNull(ColumnarNegatedQuery.asFilter(new ConstantScoreQuery(term)));
        assertNotNull(ColumnarNegatedQuery.asFilter(new BoostQuery(term, 2f)));
        assertNotNull(
            ColumnarNegatedQuery.asFilter(
                new BooleanQuery.Builder().add(term, Occur.FILTER).add(new MatchAllDocsQuery(), Occur.FILTER).build()
            )
        );
    }

    public void testAsFilterLeavesOtherExclusionsToLucene() {
        assertNull(ColumnarNegatedQuery.asFilter(new TermQuery(new Term("f", "x"))));
        assertNull(ColumnarNegatedQuery.asFilter(new MatchAllDocsQuery()));
        assertNull(ColumnarNegatedQuery.asFilter(new MatchNoDocsQuery()));
        assertNull(
            ColumnarNegatedQuery.asFilter(
                new BooleanQuery.Builder().add(new TermQuery(new Term("f", "x")), Occur.SHOULD)
                    .add(new TermQuery(new Term("f", "y")), Occur.SHOULD)
                    .build()
            )
        );
    }

    public void testRewriteOfTrivialInnerQueries() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            writer.addDocument(new Document());
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                final IndexSearcher searcher = new IndexSearcher(reader);
                assertEquals(new MatchAllDocsQuery(), new ColumnarNegatedQuery(new MatchNoDocsQuery()).rewrite(searcher));
                assertTrue(new ColumnarNegatedQuery(new MatchAllDocsQuery()).rewrite(searcher) instanceof MatchNoDocsQuery);
                final Query kept = new ColumnarNegatedQuery(new TermQuery(new Term("f", "x")));
                assertSame(kept, kept.rewrite(searcher));
            }
        }
    }

    public void testEqualsAndToString() {
        final Query a = new ColumnarNegatedQuery(new TermQuery(new Term("f", "x")));
        final Query b = new ColumnarNegatedQuery(new TermQuery(new Term("f", "x")));
        final Query other = new ColumnarNegatedQuery(new TermQuery(new Term("f", "y")));
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, other);
        assertNotEquals(a, new TermQuery(new Term("f", "x")));
        assertEquals("NOT(f:x)", a.toString());
    }

    /** What the query does not match is exactly what a prohibited clause lets through, including a segment without the field. */
    public void testMatchesTheRestIncludingAbsentValues() throws IOException {
        final int docs = between(1, 400);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int d = 0; d < docs; d++) {
                final Document doc = new Document();
                if (d % 3 != 0) {
                    doc.add(new StringField("f", d % 2 == 0 ? "even" : "odd", Field.Store.NO));
                }
                writer.addDocument(doc);
                if (rarely()) {
                    writer.flush();
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                final IndexSearcher searcher = newSearcher(reader);
                final Query even = new TermQuery(new Term("f", "even"));
                final Query prohibited = new BooleanQuery.Builder().add(new MatchAllDocsQuery(), Occur.FILTER)
                    .add(even, Occur.MUST_NOT)
                    .build();
                assertEquals(searcher.count(prohibited), searcher.count(new ColumnarNegatedQuery(even)));
                // A field no segment has: nothing is excluded, so every document matches.
                assertEquals(docs, searcher.count(new ColumnarNegatedQuery(new TermQuery(new Term("absent", "x")))));
            }
        }
    }

    /**
     * Two documents in fifty are the ones the cheap clause keeps, half of those are excluded, and the exclusion is the
     * expensive part. Both clauses can reach every document, as column scans do, so neither can skip ahead to the
     * other's candidates. Prohibited, Lucene confirms the exclusion on every document; required, only on the ones the
     * cheap clause kept.
     */
    public void testExclusionIsConfirmedOnlyOnTheDocumentsTheOtherClausesKept() throws IOException {
        final int docs = between(2_000, 6_000);
        int kept = 0;
        int survivors = 0;
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int d = 0; d < docs; d++) {
                writer.addDocument(new Document());
                if (d % 50 < 2) {
                    kept++;
                    if (d % 2 == 1) {
                        survivors++;
                    }
                }
            }
            writer.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                final IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setQueryCache(null);
                final AtomicInteger cheapConfirmations = new AtomicInteger();
                final AtomicInteger expensiveConfirmations = new AtomicInteger();
                final Query cheap = new ScanLike(d -> d % 50 < 2, 1f, cheapConfirmations);
                final Query expensive = new ScanLike(d -> d % 2 == 0, 1000f, expensiveConfirmations);

                final Query prohibited = new BooleanQuery.Builder().add(cheap, Occur.FILTER).add(expensive, Occur.MUST_NOT).build();
                assertEquals(survivors, searcher.count(prohibited));
                final int prohibitedConfirmations = expensiveConfirmations.getAndSet(0);
                cheapConfirmations.set(0);

                final Query required = new BooleanQuery.Builder().add(cheap, Occur.FILTER)
                    .add(new ColumnarNegatedQuery(expensive), Occur.FILTER)
                    .build();
                assertEquals(survivors, searcher.count(required));

                assertEquals("the cheap clause decides every document", docs, cheapConfirmations.get());
                assertEquals("only the documents the cheap clause kept are confirmed", kept, expensiveConfirmations.get());
                assertTrue(
                    "prohibited, the exclusion is confirmed far more often: " + prohibitedConfirmations + " vs " + kept,
                    prohibitedConfirmations > 5 * kept
                );
            }
        }
    }

    /**
     * A stand-in for a column scan: every document is a candidate, and a candidate is confirmed by a test that costs
     * {@code matchCost} and is counted. It does not claim to be one, so it is not a {@link ColumnarScanQuery}.
     */
    private static final class ScanLike extends Query {
        private final IntPredicate matching;
        private final float matchCost;
        private final AtomicInteger confirmations;

        ScanLike(IntPredicate matching, float matchCost, AtomicInteger confirmations) {
            this.matching = matching;
            this.matchCost = matchCost;
            this.confirmations = confirmations;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
            return new ConstantScoreWeight(this, boost) {
                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) {
                    final int maxDoc = context.reader().maxDoc();
                    return new ConstantScoreScorerSupplier(score(), scoreMode, maxDoc) {
                        @Override
                        public DocIdSetIterator iterator(long leadCost) {
                            final DocIdSetIterator all = DocIdSetIterator.all(maxDoc);
                            return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(all) {
                                @Override
                                public boolean matches() {
                                    confirmations.incrementAndGet();
                                    return matching.test(all.docID());
                                }

                                @Override
                                public float matchCost() {
                                    return matchCost;
                                }
                            });
                        }

                        @Override
                        public long cost() {
                            return maxDoc;
                        }
                    };
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return false;
                }
            };
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }

        @Override
        public String toString(String field) {
            return "ScanLike(" + matchCost + ")";
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ScanLike that && that.confirmations == confirmations && that.matchCost == matchCost;
        }

        @Override
        public int hashCode() {
            return 31 * System.identityHashCode(confirmations) + Float.hashCode(matchCost);
        }
    }
}
