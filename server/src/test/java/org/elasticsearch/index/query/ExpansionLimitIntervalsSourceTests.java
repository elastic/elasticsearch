/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.IntervalIterator;
import org.apache.lucene.queries.intervals.IntervalMatchesIterator;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class ExpansionLimitIntervalsSourceTests extends ESTestCase {

    private static final String FIELD = "field";

    /**
     * A prefix rule limited to one term, run on a segment whose only document holds three matching terms, fails with an
     * {@link IllegalArgumentException} that keeps Lucene's message and maps to a 400. Lucene counts the terms of the whole
     * segment for {@code intervals} and only the terms of the document for {@code matches}, and allows one more term in
     * {@code matches}, so the document holds three terms to exceed the limit on both paths.
     */
    public void testExpansionLimitIsIllegalArgument() throws IOException {
        IntervalsSource source = new ExpansionLimitIntervalsSource(Intervals.prefix(new BytesRef("bar"), 1));
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(new WhitespaceAnalyzer()))) {
                Document doc = new Document();
                doc.add(new TextField(FIELD, "bar1 bar2 bar3", Field.Store.NO));
                writer.addDocument(doc);
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                assertThat(reader.leaves().size(), equalTo(1));
                LeafReaderContext leaf = reader.leaves().get(0);

                IllegalArgumentException intervalsException = expectThrows(
                    IllegalArgumentException.class,
                    () -> source.intervals(FIELD, leaf)
                );
                assertExpansionLimitException(intervalsException);

                IllegalArgumentException matchesException = expectThrows(
                    IllegalArgumentException.class,
                    () -> source.matches(FIELD, leaf, 0)
                );
                assertExpansionLimitException(matchesException);
            }
        }
    }

    private static void assertExpansionLimitException(IllegalArgumentException e) {
        assertThat(e.getCause(), instanceOf(IllegalStateException.class));
        assertThat(e.getCause().getMessage(), containsString("expanded to too many terms (limit 1)"));
        assertThat(
            e.getMessage(),
            equalTo("[intervals] rule on field [" + FIELD + "] " + e.getCause().getMessage() + "; use a more selective pattern")
        );
        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testOtherIllegalStateExceptionPropagates() {
        IllegalStateException failure = new IllegalStateException("unrelated failure");
        IntervalsSource source = new ExpansionLimitIntervalsSource(new ThrowingIntervalsSource(failure));

        assertThat(expectThrows(IllegalStateException.class, () -> source.intervals(FIELD, null)), sameInstance(failure));
        assertThat(expectThrows(IllegalStateException.class, () -> source.matches(FIELD, null, 0)), sameInstance(failure));
    }

    public void testIsExpansionLimit() {
        assertTrue(
            ExpansionLimitIntervalsSource.isExpansionLimit(
                new IllegalStateException("Automaton [[0-9]{7,10}] expanded to too many terms (limit 20164)")
            )
        );
        assertFalse(ExpansionLimitIntervalsSource.isExpansionLimit(new IllegalStateException("unrelated failure")));
        assertFalse(ExpansionLimitIntervalsSource.isExpansionLimit(new IllegalStateException()));
    }

    public void testDelegation() {
        IntervalsSource inner = Intervals.ordered(Intervals.term("hello"), Intervals.term("world"));
        IntervalsSource source = new ExpansionLimitIntervalsSource(inner);

        assertThat(source.toString(), equalTo(inner.toString()));
        assertThat(source.minExtent(), equalTo(inner.minExtent()));
        assertThat(source.pullUpDisjunctions(), equalTo(List.of(source)));

        Set<String> visitedTerms = new HashSet<>();
        source.visit(FIELD, new QueryVisitor() {
            @Override
            public void consumeTerms(Query query, Term... terms) {
                for (Term term : terms) {
                    visitedTerms.add(term.text());
                }
            }
        });
        assertThat(visitedTerms, equalTo(Set.of("hello", "world")));
    }

    public void testEqualsAndHashCode() {
        String term = randomAlphaOfLength(5);
        checkEqualsAndHashCode(
            new ExpansionLimitIntervalsSource(Intervals.term(term)),
            source -> new ExpansionLimitIntervalsSource(Intervals.term(term)),
            source -> new ExpansionLimitIntervalsSource(Intervals.term(term + "x"))
        );
    }

    /**
     * Throws a given exception from {@link #intervals} and {@link #matches}. It stands in for a source failing with an
     * {@link IllegalStateException} that is not about the expansion limit, which no real source can be made to throw on demand.
     */
    private static class ThrowingIntervalsSource extends IntervalsSource {
        private final IllegalStateException failure;

        ThrowingIntervalsSource(IllegalStateException failure) {
            this.failure = failure;
        }

        @Override
        public IntervalIterator intervals(String field, LeafReaderContext ctx) {
            throw failure;
        }

        @Override
        public IntervalMatchesIterator matches(String field, LeafReaderContext ctx, int doc) {
            throw failure;
        }

        @Override
        public void visit(String field, QueryVisitor visitor) {}

        @Override
        public int minExtent() {
            return 1;
        }

        @Override
        public Collection<IntervalsSource> pullUpDisjunctions() {
            return List.of(this);
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(this);
        }

        @Override
        public boolean equals(Object other) {
            return this == other;
        }

        @Override
        public String toString() {
            return "throwing";
        }
    }
}
