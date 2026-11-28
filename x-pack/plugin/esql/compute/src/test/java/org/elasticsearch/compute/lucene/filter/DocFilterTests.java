/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.filter;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * The one-pass filter tree on real Lucene segments. Each test compares the docs that pass with a brute-force answer
 * computed from the arrays the index was built from, so a tree that reads the wrong window or keeps a deleted doc fails.
 */
public class DocFilterTests extends ComputeTestCase {
    private BlockFactory factory;
    private DocFilterFixture index;
    private int windowSize;

    @Before
    public void buildIndex() throws IOException {
        factory = blockFactory();
        windowSize = randomFrom(37, 64, 128, 4096);
        index = new DocFilterFixture();
        assertThat(index.reader.leaves().size(), equalTo(DocFilterFixture.SEGMENTS));
    }

    @After
    public void closeIndex() throws IOException {
        index.close();
    }

    public void testANativeLeafPassesTheMatchingLiveDocsOfEverySegment() throws IOException {
        try (DocFilter filter = index.category("a")) {
            assertThat(passing(filter), equalTo(index.expected(doc -> index.category[doc].equals("a"))));
        }
    }

    public void testDeletedDocsNeverPass() throws IOException {
        try (DocFilter filter = index.category("a")) {
            for (int doc : passing(filter)) {
                assertThat(index.deleted[doc], is(false));
            }
        }
    }

    public void testANativeLeafReadsEachWindowWithIntoBitSet() throws IOException {
        CountingQuery counting = new CountingQuery(new TermQuery(new Term("category", "a")));
        try (DocFilter filter = NativeDocFilter.of(index.searcher, counting)) {
            assertThat(passing(filter), equalTo(index.expected(doc -> index.category[doc].equals("a"))));
        }
        assertThat(counting.nextDocCalls.get(), equalTo(0));
        assertThat(counting.intoBitSetCalls.get(), greaterThan(0));
        assertThat(counting.advanceCalls.get(), lessThanOrEqualTo(counting.intoBitSetCalls.get()));
    }

    public void testANativeTwoPhaseQueryPassesTheConfirmedDocs() throws IOException {
        Query range = SortedNumericDocValuesField.newSlowRangeQuery("quantity", 10, 20);
        Weight weight = index.searcher.createWeight(index.searcher.rewrite(range), ScoreMode.COMPLETE_NO_SCORES, 1f);
        assertThat(weight.scorer(index.reader.leaves().get(0)).twoPhaseIterator() != null, is(true));
        try (DocFilter filter = NativeDocFilter.of(index.searcher, range)) {
            assertThat(passing(filter), equalTo(index.expected(doc -> index.quantity[doc] >= 10 && index.quantity[doc] <= 20)));
        }
    }

    public void testAnExpressionAloneEvaluatesEveryLiveDoc() throws IOException {
        try (DocFilter filter = index.expensive(new DocFilterFixture.Columns(factory), factory)) {
            assertThat(passing(filter), equalTo(index.expected(index::expensive)));
        }
    }

    public void testANullExpressionResultDoesNotPass() throws IOException {
        try (DocFilter filter = index.expensive(new DocFilterFixture.Columns(factory), factory)) {
            for (int doc : passing(filter)) {
                assertThat(index.price[doc] != null, is(true));
            }
        }
    }

    public void testANativeLeafAndAnExpressionCountTheSameDocsAsBruteForce() throws IOException {
        DocFilterFixture.Columns columns = new DocFilterFixture.Columns(factory);
        TreeSet<Integer> expected = index.expected(doc -> index.category[doc].equals("a") && index.expensive(doc));
        try (DocFilter filter = new AndDocFilter(index.category("a"), index.expensive(columns, factory))) {
            assertThat(passing(filter), equalTo(expected));
            assertThat(DocFilterPass.count(index.reader, filter, windowSize), equalTo((long) expected.size()));
        }
    }

    public void testTheExpressionLoadsOnlyDocsTheNativeLeafKept() throws IOException {
        DocFilterFixture.Columns columns = new DocFilterFixture.Columns(factory);
        try (DocFilter filter = new AndDocFilter(index.category("a"), index.expensive(columns, factory))) {
            passing(filter);
        }
        assertThat(columns.loaded.isEmpty(), is(false));
        for (int doc : columns.loaded) {
            assertThat(index.category[doc], equalTo("a"));
            assertThat(index.deleted[doc], is(false));
        }
        assertThat(columns.loaded.size(), lessThan(index.expected(doc -> true).size()));
    }

    public void testNothingIsLoadedWhenTheNativeLeafKeepsNothing() throws IOException {
        DocFilterFixture.Columns columns = new DocFilterFixture.Columns(factory);
        try (
            DocFilter filter = new AndDocFilter(
                NativeDocFilter.of(index.searcher, new TermQuery(new Term("category", "none"))),
                index.expensive(columns, factory)
            )
        ) {
            assertThat(passing(filter), is(empty()));
            assertThat(DocFilterPass.count(index.reader, filter, windowSize), equalTo(0L));
        }
        assertThat(columns.loaded, is(empty()));
    }

    public void testAnOrExpressionOnlySeesWhatTheOtherSideRejected() throws IOException {
        DocFilterFixture.Columns columns = new DocFilterFixture.Columns(factory);
        try (DocFilter filter = new OrDocFilter(index.category("a"), index.expensive(columns, factory))) {
            assertThat(passing(filter), equalTo(index.expected(doc -> index.category[doc].equals("a") || index.expensive(doc))));
        }
        assertThat(columns.loaded.isEmpty(), is(false));
        for (int doc : columns.loaded) {
            assertThat(index.category[doc], equalTo("b"));
        }
    }

    public void testAnAndInsideAnOrKeepsTheBranchesApart() throws IOException {
        DocFilterFixture.Columns columns = new DocFilterFixture.Columns(factory);
        try (
            DocFilter filter = new OrDocFilter(
                new AndDocFilter(index.category("a"), index.expensive(columns, factory)),
                new AndDocFilter(
                    index.category("b"),
                    NativeDocFilter.of(index.searcher, SortedNumericDocValuesField.newSlowRangeQuery("quantity", 1, 5))
                )
            )
        ) {
            assertThat(
                passing(filter),
                equalTo(
                    index.expected(
                        doc -> (index.category[doc].equals("a") && index.expensive(doc))
                            || (index.category[doc].equals("b") && index.quantity[doc] <= 5)
                    )
                )
            );
        }
    }

    private TreeSet<Integer> passing(DocFilter filter) throws IOException {
        TreeSet<Integer> docs = new TreeSet<>();
        DocFilterPass.run(index.reader, filter, windowSize, (window, passing) -> {
            for (int bit = passing.nextSetBit(0); bit != DocIdSetIterator.NO_MORE_DOCS; bit = next(passing, bit)) {
                docs.add(window.leaf().docBase + window.from() + bit);
            }
        });
        return docs;
    }

    private static int next(FixedBitSet bits, int bit) {
        return bit + 1 < bits.length() ? bits.nextSetBit(bit + 1) : DocIdSetIterator.NO_MORE_DOCS;
    }

    /** Counts how a native leaf walks the matching docs. */
    private static final class CountingQuery extends Query {
        private final Query in;
        private final AtomicInteger nextDocCalls = new AtomicInteger();
        private final AtomicInteger advanceCalls = new AtomicInteger();
        private final AtomicInteger intoBitSetCalls = new AtomicInteger();

        private CountingQuery(Query in) {
            this.in = in;
        }

        @Override
        public Query rewrite(IndexSearcher indexSearcher) {
            return this;
        }

        @Override
        public Weight createWeight(IndexSearcher indexSearcher, ScoreMode scoreMode, float boost) throws IOException {
            return new FilterWeight(this, in.createWeight(indexSearcher, scoreMode, boost)) {
                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    ScorerSupplier supplier = in.scorerSupplier(context);
                    if (supplier == null) {
                        return null;
                    }
                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long leadCost) throws IOException {
                            Scorer scorer = supplier.get(leadCost);
                            DocIdSetIterator iterator = new CountingIterator(scorer.iterator(), CountingQuery.this);
                            return new Scorer() {
                                @Override
                                public int docID() {
                                    return iterator.docID();
                                }

                                @Override
                                public DocIdSetIterator iterator() {
                                    return iterator;
                                }

                                @Override
                                public float score() throws IOException {
                                    return scorer.score();
                                }

                                @Override
                                public float getMaxScore(int upTo) throws IOException {
                                    return scorer.getMaxScore(upTo);
                                }
                            };
                        }

                        @Override
                        public long cost() {
                            return supplier.cost();
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
            in.visit(visitor);
        }

        @Override
        public String toString(String field) {
            return "counting(" + in.toString(field) + ")";
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(this);
        }
    }

    private static final class CountingIterator extends DocIdSetIterator {
        private final DocIdSetIterator in;
        private final CountingQuery counts;

        private CountingIterator(DocIdSetIterator in, CountingQuery counts) {
            this.in = in;
            this.counts = counts;
        }

        @Override
        public int docID() {
            return in.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            counts.nextDocCalls.incrementAndGet();
            return in.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            counts.advanceCalls.incrementAndGet();
            return in.advance(target);
        }

        @Override
        public long cost() {
            return in.cost();
        }

        @Override
        public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
            counts.intoBitSetCalls.incrementAndGet();
            in.intoBitSet(upTo, bitSet, offset);
        }
    }
}
