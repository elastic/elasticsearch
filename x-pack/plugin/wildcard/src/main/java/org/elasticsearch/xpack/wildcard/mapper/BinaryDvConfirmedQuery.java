/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.index.fielddata.MultiValuedSortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortingArrayOrderBinaryDocValues;
import org.elasticsearch.search.internal.ContextIndexSearcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Query that provided an arbitrary match across all binary doc values (but only for docs that also
 * match a provided approximation query which is key to getting good performance).
 */

abstract class BinaryDvConfirmedQuery extends Query {

    protected final String field;
    protected final Query approxQuery;
    protected final boolean arrayOrder;

    private BinaryDvConfirmedQuery(Query approximation, String field, boolean arrayOrder) {
        this.approxQuery = approximation;
        this.field = field;
        this.arrayOrder = arrayOrder;
    }

    /**
     * Returns a query that runs the generated Automaton from a range query across
     * all binary doc values (but only for docs that also match a provided approximation query which is key
     * to getting good performance). Reads the field's binary doc values using the in-order
     * {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.ArrayOrderInlineNull ArrayOrderInlineNull} format when
     * {@code arrayOrder} is {@code true} (high-cardinality columnar fields in strictly columnar index mode).
     */
    public static Query fromRangeQuery(
        Query approximation,
        String field,
        BytesRef lower,
        BytesRef upper,
        boolean includeLower,
        boolean includeUpper,
        boolean arrayOrder
    ) {
        return new BinaryDvConfirmedAutomatonQuery(
            approximation,
            field,
            new RangeAutomatonProvider(lower, upper, includeLower, includeUpper),
            arrayOrder
        );
    }

    /**
     * Returns a query that runs the generated Automaton from a wildcard query across
     * all binary doc values (but only for docs that also match a provided approximation query which is key
     * to getting good performance). Reads the field's binary doc values using the in-order
     * {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.ArrayOrderInlineNull ArrayOrderInlineNull} format when
     * {@code arrayOrder} is {@code true} (high-cardinality columnar fields in strictly columnar index mode).
     */
    public static Query fromWildcardQuery(
        Query approximation,
        String field,
        String matchPattern,
        boolean caseInsensitive,
        boolean arrayOrder
    ) {
        return new BinaryDvConfirmedAutomatonQuery(
            approximation,
            field,
            new PatternAutomatonProvider(matchPattern, caseInsensitive),
            arrayOrder
        );
    }

    /**
     * Returns a query that runs the generated Automaton from a regexp query across
     * all binary doc values (but only for docs that also match a provided approximation query which is key
     * to getting good performance). Reads the field's binary doc values using the in-order
     * {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.ArrayOrderInlineNull ArrayOrderInlineNull} format when
     * {@code arrayOrder} is {@code true} (high-cardinality columnar fields in strictly columnar index mode).
     */
    public static Query fromRegexpQuery(
        Query approximation,
        String field,
        String value,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates,
        boolean arrayOrder
    ) {
        return new BinaryDvConfirmedAutomatonQuery(
            approximation,
            field,
            new RegexAutomatonProvider(value, syntaxFlags, matchFlags, maxDeterminizedStates),
            arrayOrder
        );
    }

    /**
     * Returns a query that runs the generated Automaton from a fuzzy query across
     * all binary doc values (but only for docs that also match a provided approximation query which is key
     * to getting good performance). Reads the field's binary doc values using the in-order
     * {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.ArrayOrderInlineNull ArrayOrderInlineNull} format when
     * {@code arrayOrder} is {@code true} (high-cardinality columnar fields in strictly columnar index mode).
     */
    public static Query fromFuzzyQuery(Query approximation, String field, String searchTerm, FuzzyQuery fuzzyQuery, boolean arrayOrder) {
        return new BinaryDvConfirmedAutomatonQuery(
            approximation,
            field,
            new FuzzyQueryAutomatonProvider(searchTerm, fuzzyQuery),
            arrayOrder
        );
    }

    /**
     * Returns a query that checks for equality of at least one of the provided terms across
     * all binary doc values (but only for docs that also match a provided approximation query which
     * is key to getting good performance). Reads the field's binary doc values using the in-order
     * {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.ArrayOrderInlineNull ArrayOrderInlineNull} format when
     * {@code arrayOrder} is {@code true} (high-cardinality columnar fields in strictly columnar index mode).
     */
    public static Query fromTerms(Query approximation, String field, boolean arrayOrder, BytesRef... terms) {
        Arrays.sort(terms, BytesRef::compareTo);
        return new BinaryDvConfirmedTermsQuery(approximation, field, terms, arrayOrder);
    }

    protected abstract BinaryDVMatcher getBinaryDVMatcher();

    protected abstract Query rewrite(Query approxRewrite) throws IOException;

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Query approxRewrite = approxQuery.rewrite(searcher);
        if (approxRewrite instanceof MatchNoDocsQuery) {
            return MatchNoDocsQuery.INSTANCE;
        }
        if (approxQuery != approxRewrite) {
            return rewrite(approxRewrite);
        }
        return this;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        final Weight approxWeight = approxQuery.createWeight(searcher, scoreMode, boost);
        final BinaryDVMatcher matcher = getBinaryDVMatcher();
        final CircuitBreaker breaker = ContextIndexSearcher.circuitBreakerOrNull(searcher);
        return new ConstantScoreWeight(this, boost) {

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                ScorerSupplier approxScorerSupplier = approxWeight.scorerSupplier(context);
                if (approxScorerSupplier == null) {
                    // No matches to be had
                    return null;
                }

                // Checkpoint before opening the binary doc values reader for this surviving clause/segment pair.
                ContextIndexSearcher.checkBinaryDvDecodeBreaker(breaker);

                final SortedBinaryDocValues values = arrayOrder
                    ? SortingArrayOrderBinaryDocValues.from(context.reader(), field)
                    : MultiValuedSortedBinaryDocValues.fromMultiValued(context.reader(), field);

                return new ScorerSupplier() {
                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        final Scorer approxScorer = approxScorerSupplier.get(leadCost);
                        final DocIdSetIterator approxDisi = approxScorer.iterator();
                        final TwoPhaseIterator twoPhase = new TwoPhaseIterator(approxDisi) {
                            @Override
                            public boolean matches() throws IOException {
                                if (values.advanceExact(approxDisi.docID()) == false) {
                                    // Can happen when approxQuery resolves to some form of MatchAllDocs expression
                                    return false;
                                }
                                return matcher.matchesBinaryDV(values);
                            }

                            @Override
                            public float matchCost() {
                                // TODO: how can we compute this?
                                return 1000f;
                            }
                        };
                        return new ConstantScoreScorer(score(), scoreMode, twoPhase);
                    }

                    @Override
                    public long cost() {
                        return approxScorerSupplier.cost();
                    }
                };
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }
        };
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        BinaryDvConfirmedQuery other = (BinaryDvConfirmedQuery) obj;
        return arrayOrder == other.arrayOrder && Objects.equals(field, other.field) && Objects.equals(approxQuery, other.approxQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, approxQuery, arrayOrder);
    }

    Query getApproximationQuery() {
        return approxQuery;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            approxQuery.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
            visitor.visitLeaf(this);
        }
    }

    interface BinaryDVMatcher {
        boolean matchesBinaryDV(SortedBinaryDocValues values) throws IOException;
    }

    private static class BinaryDvConfirmedAutomatonQuery extends BinaryDvConfirmedQuery {

        private final AutomatonProvider automatonProvider;

        private BinaryDvConfirmedAutomatonQuery(
            Query approximation,
            String field,
            AutomatonProvider automatonProvider,
            boolean arrayOrder
        ) {
            super(approximation, field, arrayOrder);
            this.automatonProvider = automatonProvider;
        }

        @Override
        protected BinaryDVMatcher getBinaryDVMatcher() {
            final ByteRunAutomaton byteRunAutomaton = new ByteRunAutomaton(automatonProvider.getAutomaton(field));
            return (values) -> {
                int count = values.docValueCount();
                for (int i = 0; i < count; i++) {
                    BytesRef val = values.nextValue();
                    if (byteRunAutomaton.run(val.bytes, val.offset, val.length)) {
                        return true;
                    }
                }
                return false;
            };
        }

        @Override
        protected Query rewrite(Query approxRewrite) {
            return new BinaryDvConfirmedAutomatonQuery(approxRewrite, field, automatonProvider, arrayOrder);
        }

        @Override
        public String toString(String field) {
            return field + ":" + automatonProvider.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            if (super.equals(o) == false) return false;
            BinaryDvConfirmedAutomatonQuery other = (BinaryDvConfirmedAutomatonQuery) o;
            return Objects.equals(automatonProvider, other.automatonProvider);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), automatonProvider);
        }
    }

    private static class BinaryDvConfirmedTermsQuery extends BinaryDvConfirmedQuery {

        private final BytesRef[] terms;

        private BinaryDvConfirmedTermsQuery(Query approximation, String field, BytesRef[] terms, boolean arrayOrder) {
            super(approximation, field, arrayOrder);
            // terms must already be sorted
            this.terms = terms;
        }

        @Override
        protected BinaryDVMatcher getBinaryDVMatcher() {
            return (values) -> {
                int count = values.docValueCount();
                for (int i = 0; i < count; i++) {
                    BytesRef val = values.nextValue();
                    if (terms.length == 1) {
                        if (terms[0].bytesEquals(val)) {
                            return true;
                        }
                    } else {
                        final int pos = Arrays.binarySearch(terms, val, BytesRef::compareTo);
                        if (pos >= 0) {
                            assert terms[pos].bytesEquals(val) : "Expected term at position " + pos + " to match value, but it did not.";
                            return true;
                        }
                    }
                }
                return false;
            };
        }

        @Override
        protected Query rewrite(Query approxRewrite) {
            return new BinaryDvConfirmedTermsQuery(approxRewrite, field, terms, arrayOrder);
        }

        @Override
        public String toString(String field) {
            StringBuilder builder = new StringBuilder(field + ": [");
            for (int i = 0; i < terms.length; i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                builder.append(terms[i].utf8ToString());
            }
            builder.append("]");
            return builder.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            if (super.equals(o) == false) return false;
            BinaryDvConfirmedTermsQuery that = (BinaryDvConfirmedTermsQuery) o;
            return Arrays.equals(this.terms, that.terms);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), Arrays.hashCode(terms));
        }
    }

    private interface AutomatonProvider {
        Automaton getAutomaton(String field);
    }

    private record PatternAutomatonProvider(String matchPattern, boolean caseInsensitive) implements AutomatonProvider {
        @Override
        public Automaton getAutomaton(String field) {
            return caseInsensitive
                ? AutomatonQueries.toCaseInsensitiveWildcardAutomaton(new Term(field, matchPattern))
                : WildcardQuery.toAutomaton(new Term(field, matchPattern), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        }
    }

    private record RegexAutomatonProvider(String value, int syntaxFlags, int matchFlags, int maxDeterminizedStates)
        implements
            AutomatonProvider {
        @Override
        public Automaton getAutomaton(String field) {
            RegExp regex = new RegExp(value, syntaxFlags, matchFlags);
            return Operations.determinize(regex.toAutomaton(), maxDeterminizedStates);
        }
    }

    private record RangeAutomatonProvider(BytesRef lower, BytesRef upper, boolean includeLower, boolean includeUpper)
        implements
            AutomatonProvider {
        @Override
        public Automaton getAutomaton(String field) {
            return TermRangeQuery.toAutomaton(lower, upper, includeLower, includeUpper);
        }
    }

    private record FuzzyQueryAutomatonProvider(String searchTerm, FuzzyQuery fuzzyQuery) implements AutomatonProvider {
        @Override
        public Automaton getAutomaton(String field) {
            return fuzzyQuery.getAutomata().automaton;
        }
    }
}
