/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.breaker.TrackingCircuitBreaker;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.lucene.queries.BinaryDocValuesScanCost;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

public class BinaryDvConfirmedQueryTests extends ESTestCase {

    public void testIsAccountedForAsABinaryDocValuesScanCost() {
        Query query = BinaryDvConfirmedQuery.fromWildcardQuery(
            Queries.ALL_DOCS_INSTANCE,
            "field",
            "*",
            false,
            false,
            NoopCircuitBreaker.INSTANCE
        );

        assertThat(
            "every matches() call opens a decoder over the field's full binary doc values, same as the Scanning* queries",
            query,
            instanceOf(BinaryDocValuesScanCost.class)
        );
        assertEquals("field", ((BinaryDocValuesScanCost) query).field());
    }

    public void testNoBinaryDocValuesOpenedDuringPlanning() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                final Document document = new Document();
                document.add(new BinaryDocValuesField("field", new BytesRef("hello")));
                writer.addDocument(document);
                try (DirectoryReader reader = forbidBinaryDvOpenReader(writer.getReader())) {
                    final IndexSearcher searcher = new IndexSearcher(reader);
                    final Query query = BinaryDvConfirmedQuery.fromWildcardQuery(
                        Queries.ALL_DOCS_INSTANCE,
                        "field",
                        "*",
                        false,
                        false,
                        NoopCircuitBreaker.INSTANCE
                    );
                    final Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
                    for (LeafReaderContext ctx : reader.leaves()) {
                        weight.scorerSupplier(ctx);
                    }
                }
            }
        }
    }

    /**
     * The fuzzy automaton is already UTF-8 byte-level, so wrapping its {@code automaton} in a fresh {@code ByteRunAutomaton} converts it
     * a second time and mis-matches non-ASCII values. It must run the pre-compiled {@code runAutomaton}.
     */
    public void testFuzzyMatchesNonAsciiValues() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                for (String value : new String[] { "héllo", "hello", "hèllo", "world" }) {
                    final Document document = new Document();
                    final BytesRef encoded = MultiValuedBinaryDocValuesField.IntegratedCount.encode(List.of(new BytesRef(value)));
                    document.add(new BinaryDocValuesField("field", encoded));
                    writer.addDocument(document);
                }
                try (DirectoryReader reader = writer.getReader()) {
                    final IndexSearcher searcher = new IndexSearcher(reader);
                    final FuzzyQuery fuzzy = new FuzzyQuery(new Term("field", "héllo"), 1, 0, 50, true);
                    final Query query = BinaryDvConfirmedQuery.fromFuzzyQuery(
                        Queries.ALL_DOCS_INSTANCE,
                        "field",
                        "héllo",
                        fuzzy,
                        false,
                        NoopCircuitBreaker.INSTANCE
                    );
                    // "world" is more than one edit away; the other three are within one edit of the search term
                    assertThat(searcher.count(query), equalTo(3));
                }
            }
        }
    }

    private static final String TOO_COMPLEX_WILDCARD = "*a????????????*";

    private static final String TOO_COMPLEX_REGEXP = "[ac]*a[ac]{200,500}";

    public void testTooComplexWildcardPatternThrowsBadRequest() {
        for (boolean caseInsensitive : new boolean[] { false, true }) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> BinaryDvConfirmedQuery.fromWildcardQuery(
                    Queries.ALL_DOCS_INSTANCE,
                    "field",
                    TOO_COMPLEX_WILDCARD,
                    caseInsensitive,
                    false,
                    NoopCircuitBreaker.INSTANCE
                )
            );
            assertTooComplex(e);
        }
    }

    public void testTooComplexRegexpPatternThrowsBadRequest() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> BinaryDvConfirmedQuery.fromRegexpQuery(
                Queries.ALL_DOCS_INSTANCE,
                "field",
                TOO_COMPLEX_REGEXP,
                RegExp.ALL,
                0,
                Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                false,
                NoopCircuitBreaker.INSTANCE
            )
        );
        assertTooComplex(e);
    }

    private static void assertTooComplex(IllegalArgumentException e) {
        assertThat(e.getMessage(), equalTo("Pattern was too complex to determinize"));
        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getCause(), instanceOf(TooComplexToDeterminizeException.class));
    }

    // '*' + 65 'a's: subset construction creates 66 DFA states, CB fires at state 64.
    private static final String COMPLEX_WILDCARD = "*" + "a".repeat(65);

    public void testCircuitBreakerConsultedForWildcardDuringQueryConstruction() {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker();
        BinaryDvConfirmedQuery.fromWildcardQuery(Queries.ALL_DOCS_INSTANCE, "field", COMPLEX_WILDCARD, randomBoolean(), false, breaker);
        assertTrue("circuit breaker should be consulted during wildcard automaton construction", breaker.wasCalled());
    }

    public void testWildcardChargesBreakerForByteRunAutomatonBuild() throws IOException {
        final boolean caseInsensitive = randomBoolean();
        final String pattern = "h*l?o*" + randomAlphaOfLength(5);
        final Term term = new Term("field", pattern);
        final Automaton dfa = caseInsensitive
            ? AutomatonQueries.toCaseInsensitiveWildcardAutomaton(term)
            : WildcardQuery.toAutomaton(term, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        assertChargesTwiceAutomatonRam(
            breaker -> BinaryDvConfirmedQuery.fromWildcardQuery(
                Queries.ALL_DOCS_INSTANCE,
                "field",
                pattern,
                caseInsensitive,
                false,
                breaker
            ),
            dfa
        );
    }

    public void testRegexpChargesBreakerForByteRunAutomatonBuild() throws IOException {
        final String pattern = "h.*l[a-z]o" + randomAlphaOfLength(5);
        final Automaton dfa = Operations.determinize(
            new RegExp(pattern, RegExp.ALL, 0).toAutomaton(),
            Operations.DEFAULT_DETERMINIZE_WORK_LIMIT
        );
        assertChargesTwiceAutomatonRam(
            breaker -> BinaryDvConfirmedQuery.fromRegexpQuery(
                Queries.ALL_DOCS_INSTANCE,
                "field",
                pattern,
                RegExp.ALL,
                0,
                Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                false,
                breaker
            ),
            dfa
        );
    }

    public void testRangeChargesBreakerForByteRunAutomatonBuild() throws IOException {
        final BytesRef lower = new BytesRef("a" + randomAlphaOfLength(5));
        final BytesRef upper = new BytesRef("z" + randomAlphaOfLength(5));
        final Automaton dfa = TermRangeQuery.toAutomaton(lower, upper, true, true);
        assertChargesTwiceAutomatonRam(
            breaker -> BinaryDvConfirmedQuery.fromRangeQuery(Queries.ALL_DOCS_INSTANCE, "field", lower, upper, true, true, false, breaker),
            dfa
        );
    }

    public void testSuppliedAutomatonChargesBreakerForByteRunAutomatonBuild() throws IOException {
        final Automaton dfa = Operations.determinize(
            Operations.union(List.of(Automata.makeString("hello"), Automata.makeString("world"))),
            Operations.DEFAULT_DETERMINIZE_WORK_LIMIT
        );
        assertChargesTwiceAutomatonRam(
            breaker -> BinaryDvConfirmedQuery.fromAutomaton(Queries.ALL_DOCS_INSTANCE, "field", () -> dfa, "hello|world", false, breaker),
            dfa
        );
    }

    /**
     * Building the query must have charged the breaker, at some point, for at least twice the RAM of the automaton it converts to a
     * {@code ByteRunAutomaton}, and must have released everything afterwards. The automaton it retains is not charged by the query
     * itself but reported through {@link Accountable}, for {@code MaxClauseCountQueryVisitor} to charge.
     */
    private void assertChargesTwiceAutomatonRam(Function<CircuitBreaker, Query> queryBuilder, Automaton automaton) {
        final TrackingCircuitBreaker breaker = new TrackingCircuitBreaker();
        final Query query = queryBuilder.apply(breaker);
        assertThat(breaker.peak(), greaterThanOrEqualTo(2 * automaton.ramBytesUsed()));
        assertThat(breaker.getUsed(), equalTo(0L));
        assertThat(query, instanceOf(Accountable.class));
        assertThat(((Accountable) query).ramBytesUsed(), greaterThan(0L));
    }

    private static DirectoryReader forbidBinaryDvOpenReader(DirectoryReader reader) throws IOException {
        return new FilterDirectoryReader(reader, new FilterDirectoryReader.SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader leaf) {
                return new FilterLeafReader(leaf) {
                    @Override
                    public BinaryDocValues getBinaryDocValues(String field) {
                        throw new AssertionError(
                            "getBinaryDocValues() must not be called during scorerSupplier() (planning phase);"
                                + " defer reader construction to ScorerSupplier#get(). field=["
                                + field
                                + "]"
                        );
                    }

                    @Override
                    public IndexReader.CacheHelper getCoreCacheHelper() {
                        return null;
                    }

                    @Override
                    public IndexReader.CacheHelper getReaderCacheHelper() {
                        return null;
                    }
                };
            }
        }) {
            @Override
            protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
                return in;
            }

            @Override
            public IndexReader.CacheHelper getReaderCacheHelper() {
                return null;
            }
        };
    }
}
