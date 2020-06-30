/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

public final class StringRuntimeValues extends AbstractRuntimeValues<StringRuntimeValues.SharedValues> {
    @FunctionalInterface
    public interface NewLeafLoader {
        IntConsumer leafLoader(LeafReaderContext ctx, Consumer<String> sync) throws IOException;
    }

    private final NewLeafLoader newLeafLoader;

    public StringRuntimeValues(NewLeafLoader newLeafLoader) {
        this.newLeafLoader = newLeafLoader;
    }

    public CheckedFunction<LeafReaderContext, SortedBinaryDocValues, IOException> docValues() {
        return unstarted().docValues();
    }

    public Query fuzzyQuery(String fieldName, String value, int maxEdits, int prefixLength, int maxExpansions, boolean transpositions) {
        return unstarted().new FuzzyQuery(fieldName, value, maxEdits, prefixLength, maxExpansions, transpositions);
    }

    public Query prefixQuery(String fieldName, String value) {
        return unstarted().new PrefixQuery(fieldName, value);
    }

    public Query termQuery(String fieldName, String value) {
        return unstarted().new TermQuery(fieldName, value);
    }

    public Query termsQuery(String fieldName, String... values) {
        return unstarted().new TermsQuery(fieldName, values);
    }

    public Query rangeQuery(String fieldName, String lowerValue, String upperValue) {
        return unstarted().new RangeQuery(fieldName, lowerValue, upperValue);
    }

    public Query wildcardQuery(String fieldName, String pattern) {
        return unstarted().new WildcardQuery(fieldName, pattern);
    }

    @Override
    protected SharedValues newSharedValues() {
        return new SharedValues();
    }

    protected class SharedValues extends AbstractRuntimeValues<SharedValues>.SharedValues {
        private String[] values = new String[1];

        @Override
        protected IntConsumer newLeafLoader(LeafReaderContext ctx) throws IOException {
            return newLeafLoader.leafLoader(ctx, this::add);
        }

        private void add(String value) {
            if (value == null) {
                return;
            }
            int newCount = count + 1;
            if (values.length < newCount) {
                values = Arrays.copyOf(values, ArrayUtil.oversize(newCount, RamUsageEstimator.NUM_BYTES_OBJECT_REF));
            }
            values[count] = value;
            count = newCount;
        }

        @Override
        protected void sort() {
            Arrays.sort(values, 0, count);
        }

        private CheckedFunction<LeafReaderContext, SortedBinaryDocValues, IOException> docValues() {
            alwaysSortResults();
            return DocValues::new;
        }

        private class DocValues extends SortedBinaryDocValues {
            private final BytesRefBuilder ref = new BytesRefBuilder();
            private final IntConsumer leafCursor;
            private int next;

            DocValues(LeafReaderContext ctx) throws IOException {
                leafCursor = leafCursor(ctx);
            }

            @Override
            public boolean advanceExact(int docId) throws IOException {
                leafCursor.accept(docId);
                next = 0;
                return count > 0;
            }

            @Override
            public int docValueCount() {
                return count;
            }

            @Override
            public BytesRef nextValue() throws IOException {
                ref.copyChars(values[next++]);
                return ref.get();
            }
        }

        private class FuzzyQuery extends AbstractRuntimeQuery {
            private final BytesRefBuilder scratch = new BytesRefBuilder();
            private final String term;
            private final org.apache.lucene.search.FuzzyQuery delegate;
            private final CompiledAutomaton automaton;

            private FuzzyQuery(String fieldName, String term, int maxEdits, int prefixLength, int maxExpansions, boolean transpositions) {
                super(fieldName);
                this.term = term;
                delegate = new org.apache.lucene.search.FuzzyQuery(
                    new Term(fieldName, term),
                    maxEdits,
                    prefixLength,
                    maxExpansions,
                    transpositions
                );
                automaton = delegate.getAutomata();
                if (automaton.type != CompiledAutomaton.AUTOMATON_TYPE.NORMAL) {
                    throw new IllegalArgumentException("Can't compile automaton for [" + delegate + "]");
                }
            }

            @Override
            protected boolean matches() {
                for (int i = 0; i < count; i++) {
                    scratch.copyChars(values[i]);
                    if (automaton.runAutomaton.run(scratch.bytes(), 0, scratch.length())) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void visit(QueryVisitor visitor) {
                visitor.consumeTerms(this, new Term(fieldName, term));
            }

            @Override
            protected String bareToString() {
                return term + "~" + delegate.getMaxEdits();
            }

            @Override
            public int hashCode() {
                return Objects.hash(super.hashCode(), term);
            }

            @Override
            public boolean equals(Object obj) {
                if (false == super.equals(obj)) {
                    return false;
                }
                FuzzyQuery other = (FuzzyQuery) obj;
                return delegate.equals(other.delegate);
            }
        }

        private class PrefixQuery extends AbstractRuntimeQuery {
            private final String prefix;

            private PrefixQuery(String fieldName, String prefix) {
                super(fieldName);
                this.prefix = prefix;
            }

            @Override
            protected boolean matches() {
                for (int i = 0; i < count; i++) {
                    if (values[i].startsWith(prefix)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            protected String bareToString() {
                return prefix + "*";
            }

            @Override
            public void visit(QueryVisitor visitor) {
                visitor.consumeTermsMatching(
                    this,
                    fieldName,
                    () -> new ByteRunAutomaton(
                        org.apache.lucene.search.PrefixQuery.toAutomaton(new BytesRef(prefix)),
                        true,
                        Integer.MAX_VALUE
                    )
                );
            }

            @Override
            public int hashCode() {
                return Objects.hash(super.hashCode(), prefix);
            }

            @Override
            public boolean equals(Object obj) {
                if (false == super.equals(obj)) {
                    return false;
                }
                PrefixQuery other = (PrefixQuery) obj;
                return prefix.equals(other.prefix);
            }
        }

        private class TermQuery extends AbstractRuntimeQuery {
            private final String term;

            private TermQuery(String fieldName, String term) {
                super(fieldName);
                this.term = term;
            }

            @Override
            protected boolean matches() {
                for (int i = 0; i < count; i++) {
                    if (term.equals(values[i])) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void visit(QueryVisitor visitor) {
                visitor.consumeTerms(this, new Term(fieldName, term));
            }

            @Override
            protected String bareToString() {
                return term;
            }

            @Override
            public int hashCode() {
                return Objects.hash(super.hashCode(), term);
            }

            @Override
            public boolean equals(Object obj) {
                if (false == super.equals(obj)) {
                    return false;
                }
                TermQuery other = (TermQuery) obj;
                return term.equals(other.term);
            }
        }

        private class TermsQuery extends AbstractRuntimeQuery {
            private final String[] terms;

            private TermsQuery(String fieldName, String[] terms) {
                super(fieldName);
                this.terms = terms.clone();
                Arrays.sort(terms);
            }

            @Override
            protected boolean matches() {
                for (int i = 0; i < count; i++) {
                    if (Arrays.binarySearch(terms, values[i]) >= 0) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void visit(QueryVisitor visitor) {
                for (String term : terms) {
                    visitor.consumeTerms(this, new Term(fieldName, term));
                }
            }

            @Override
            protected String bareToString() {
                return "{" + Arrays.toString(terms) + "}";
            }

            @Override
            public int hashCode() {
                return Objects.hash(super.hashCode(), Arrays.hashCode(terms));
            }

            @Override
            public boolean equals(Object obj) {
                if (false == super.equals(obj)) {
                    return false;
                }
                TermsQuery other = (TermsQuery) obj;
                return Arrays.equals(terms, other.terms);
            }
        }

        private class RangeQuery extends AbstractRuntimeQuery {
            private final String lowerValue;
            private final String upperValue;

            private RangeQuery(String fieldName, String lowerValue, String upperValue) {
                super(fieldName);
                this.lowerValue = lowerValue;
                this.upperValue = upperValue;
                assert lowerValue.compareTo(upperValue) <= 0;
            }

            @Override
            protected boolean matches() {
                for (int i = 0; i < count; i++) {
                    if (lowerValue.compareTo(values[i]) <= 0 && upperValue.compareTo(values[i]) >= 0) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            protected String bareToString() {
                return "[" + lowerValue + "," + upperValue + "]";
            }

            @Override
            public int hashCode() {
                return Objects.hash(super.hashCode(), lowerValue, upperValue);
            }

            @Override
            public boolean equals(Object obj) {
                if (false == super.equals(obj)) {
                    return false;
                }
                RangeQuery other = (RangeQuery) obj;
                return lowerValue.equals(other.lowerValue) && upperValue.equals(other.upperValue);
            }
        }

        private class WildcardQuery extends AbstractRuntimeQuery {
            private final BytesRefBuilder scratch = new BytesRefBuilder();
            private final String pattern;
            private final ByteRunAutomaton automaton;

            private WildcardQuery(String fieldName, String pattern) {
                super(fieldName);
                this.pattern = pattern;
                automaton = new ByteRunAutomaton(org.apache.lucene.search.WildcardQuery.toAutomaton(new Term(fieldName, pattern)));
            }

            @Override
            protected boolean matches() {
                for (int i = 0; i < count; i++) {
                    scratch.copyChars(values[i]);
                    if (automaton.run(scratch.bytes(), 0, scratch.length())) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            protected String bareToString() {
                return pattern;
            }

            @Override
            public void visit(QueryVisitor visitor) {
                if (visitor.acceptField(fieldName)) {
                    visitor.consumeTermsMatching(this, fieldName, () -> automaton);
                }
            }

            @Override
            public int hashCode() {
                return Objects.hash(super.hashCode(), pattern);
            }

            @Override
            public boolean equals(Object obj) {
                if (false == super.equals(obj)) {
                    return false;
                }
                WildcardQuery other = (WildcardQuery) obj;
                return pattern.equals(other.pattern);
            }
        }
    }
}
