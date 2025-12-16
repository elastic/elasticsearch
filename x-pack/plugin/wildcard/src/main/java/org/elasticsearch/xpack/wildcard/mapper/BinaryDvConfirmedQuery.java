/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
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
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.lucene.search.AutomatonQueries;

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

    private BinaryDvConfirmedQuery(Query approximation, String field) {
        this.approxQuery = approximation;
        this.field = field;
    }

    /**
     * Returns a query that runs the generated Automaton from a range query across
     * all binary doc values (but only for docs that also match a provided approximation query which is key
     * to getting good performance).
     */
    public static Query fromRangeQuery(
        Query approximation,
        String field,
        BytesRef lower,
        BytesRef upper,
        boolean includeLower,
        boolean includeUpper
    ) {
        return new BinaryDvConfirmedAutomatonQuery(
            approximation,
            field,
            new RangeAutomatonProvider(lower, upper, includeLower, includeUpper)
        );
    }

    /**
     * Returns a query that runs the generated Automaton from a wildcard query across
     * all binary doc values (but only for docs that also match a provided approximation query which is key
     * to getting good performance).
     */
    public static Query fromWildcardQuery(Query approximation, String field, String matchPattern, boolean caseInsensitive) {
        return new BinaryDvConfirmedAutomatonQuery(approximation, field, new PatternAutomatonProvider(matchPattern, caseInsensitive));
    }

    /**
     * Returns a query that runs the generated Automaton from a regexp query across
     * all binary doc values (but only for docs that also match a provided approximation query which is key
     * to getting good performance).
     */
    public static Query fromRegexpQuery(
        Query approximation,
        String field,
        String value,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates
    ) {
        return new BinaryDvConfirmedAutomatonQuery(
            approximation,
            field,
            new RegexAutomatonProvider(value, syntaxFlags, matchFlags, maxDeterminizedStates)
        );
    }

    /**
     * Returns a query that runs the generated Automaton from a fuzzy query across
     * all binary doc values (but only for docs that also match a provided approximation query which is key
     * to getting good performance).
     */
    public static Query fromFuzzyQuery(Query approximation, String field, String searchTerm, FuzzyQuery fuzzyQuery) {
        return new BinaryDvConfirmedAutomatonQuery(approximation, field, new FuzzyQueryAutomatonProvider(searchTerm, fuzzyQuery));
    }

    /**
     * Returns a query that checks for equality of at least one of the provided terms across
     * all binary doc values (but only for docs that also match a provided approximation query which
     * is key to getting good performance).
     */
    public static Query fromTerms(Query approximation, String field, BytesRef... terms) {
        Arrays.sort(terms, BytesRef::compareTo);
        return new BinaryDvConfirmedTermsQuery(approximation, field, terms);
    }

    protected abstract BinaryDVMatcher getBinaryDVMatcher();

    protected abstract Query rewrite(Query approxRewrite) throws IOException;

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Query approxRewrite = approxQuery.rewrite(searcher);
        if (approxQuery != approxRewrite) {
            return rewrite(approxRewrite);
        }
        return this;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        final Weight approxWeight = approxQuery.createWeight(searcher, scoreMode, boost);
        final BinaryDVMatcher matcher = getBinaryDVMatcher();
        return new ConstantScoreWeight(this, boost) {

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                ScorerSupplier approxScorerSupplier = approxWeight.scorerSupplier(context);
                if (approxScorerSupplier == null) {
                    // No matches to be had
                    return null;
                }
                final ByteArrayStreamInput bytes = new ByteArrayStreamInput();
                final BytesRef scratch = new BytesRef();
                final BinaryDocValues values = DocValues.getBinary(context.reader(), field);
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
                                final BytesRef bytesRef = values.binaryValue();
                                bytes.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                                return matcher.matchesBinaryDV(bytes, bytesRef, scratch);
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
        return Objects.equals(field, other.field) && Objects.equals(approxQuery, other.approxQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, approxQuery);
    }

    Query getApproximationQuery() {
        return approxQuery;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    interface BinaryDVMatcher {
        boolean matchesBinaryDV(ByteArrayStreamInput bytes, BytesRef bytesRef, BytesRef scratch) throws IOException;
    }

    private static class BinaryDvConfirmedAutomatonQuery extends BinaryDvConfirmedQuery {

        private final AutomatonProvider automatonProvider;

        private BinaryDvConfirmedAutomatonQuery(Query approximation, String field, AutomatonProvider automatonProvider) {
            super(approximation, field);
            this.automatonProvider = automatonProvider;
        }

        @Override
        protected BinaryDVMatcher getBinaryDVMatcher() {
            final ByteRunAutomaton byteRunAutomaton = new ByteRunAutomaton(automatonProvider.getAutomaton(field));
            return (bytes, bytesRef, scratch) -> {
                final int size = bytes.readVInt();
                for (int i = 0; i < size; i++) {
                    final int valLength = bytes.readVInt();
                    if (byteRunAutomaton.run(bytesRef.bytes, bytes.getPosition(), valLength)) {
                        return true;
                    }
                    bytes.skipBytes(valLength);
                }
                return false;
            };
        }

        @Override
        protected Query rewrite(Query approxRewrite) {
            return new BinaryDvConfirmedAutomatonQuery(approxRewrite, field, automatonProvider);
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

        private BinaryDvConfirmedTermsQuery(Query approximation, String field, BytesRef[] terms) {
            super(approximation, field);
            // terms must already be sorted
            this.terms = terms;
        }

        @Override
        protected BinaryDVMatcher getBinaryDVMatcher() {
            return (bytes, bytesRef, scratch) -> {
                scratch.bytes = bytesRef.bytes;
                final int size = bytes.readVInt();
                for (int i = 0; i < size; i++) {
                    final int valLength = bytes.readVInt();
                    scratch.offset = bytes.getPosition();
                    scratch.length = valLength;
                    if (terms.length == 1) {
                        if (terms[0].bytesEquals(scratch)) {
                            return true;
                        }
                    } else {
                        final int pos = Arrays.binarySearch(terms, scratch, BytesRef::compareTo);
                        if (pos >= 0) {
                            assert terms[pos].bytesEquals(scratch)
                                : "Expected term at position " + pos + " to match scratch, but it did not.";
                            return true;
                        }
                    }
                    bytes.skipBytes(valLength);
                }
                assert bytes.available() == 0 : "Expected no bytes left to read, but found " + bytes.available();
                return false;
            };
        }

        @Override
        protected Query rewrite(Query approxRewrite) {
            return new BinaryDvConfirmedTermsQuery(approxRewrite, field, terms);
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
