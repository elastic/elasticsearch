/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.CheckedFunction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

/**
 * Manages the creation of doc values and queries for {@code long} fields.
 */
public final class LongRuntimeFieldHelper {
    @FunctionalInterface
    public interface NewLeafLoader {
        IntConsumer leafLoader(LeafReaderContext ctx, LongConsumer sync) throws IOException;
    }

    private final NewLeafLoader newLeafLoader;

    public LongRuntimeFieldHelper(NewLeafLoader newLeafLoader) {
        this.newLeafLoader = newLeafLoader;
    }

    public CheckedFunction<LeafReaderContext, SortedNumericDocValues, IOException> docValues() {
        return new Values().docValues();
    }

    public Query existsQuery(String fieldName) {
        return new Values().new ExistsQuery(fieldName);
    }

    public Query rangeQuery(String fieldName, long lowerValue, long upperValue) {
        return new Values().new RangeQuery(fieldName, lowerValue, upperValue);
    }

    public Query termQuery(String fieldName, long value) {
        return new Values().new TermQuery(fieldName, value);
    }

    public Query termsQuery(String fieldName, long... value) {
        return new Values().new TermsQuery(fieldName, value);
    }

    private class Values extends AbstractRuntimeValues {
        private long[] values = new long[1];

        @Override
        protected IntConsumer newLeafLoader(LeafReaderContext ctx) throws IOException {
            return newLeafLoader.leafLoader(ctx, this::add);
        }

        private void add(long value) {
            int newCount = count + 1;
            if (values.length < newCount) {
                values = Arrays.copyOf(values, ArrayUtil.oversize(newCount, 8));
            }
            values[count] = value;
            count = newCount;
        }

        @Override
        protected void sort() {
            Arrays.sort(values, 0, count);
        }

        private CheckedFunction<LeafReaderContext, SortedNumericDocValues, IOException> docValues() {
            alwaysSortResults();
            return DocValues::new;
        }

        private class DocValues extends SortedNumericDocValues {
            private final IntConsumer leafCursor;
            private int next;

            DocValues(LeafReaderContext ctx) throws IOException {
                leafCursor = leafCursor(ctx);
            }

            @Override
            public long nextValue() throws IOException {
                return values[next++];
            }

            @Override
            public int docValueCount() {
                return count;
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                leafCursor.accept(target);
                next = 0;
                return count > 0;
            }

            @Override
            public int docID() {
                return docId();
            }

            @Override
            public int nextDoc() throws IOException {
                return advance(docId() + 1);
            }

            @Override
            public int advance(int target) throws IOException {
                int current = target;
                while (current < maxDoc()) {
                    if (advanceExact(current)) {
                        return current;
                    }
                    current++;
                }
                return NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                // TODO we have no idea what this should be and no real way to get one
                return 1000;
            }
        }

        private class ExistsQuery extends AbstractRuntimeQuery {
            private ExistsQuery(String fieldName) {
                super(fieldName);
            }

            @Override
            protected boolean matches() {
                return count > 0;
            }

            @Override
            protected String bareToString() {
                return "*";
            }
        }

        private class RangeQuery extends AbstractRuntimeQuery {
            private final long lowerValue;
            private final long upperValue;

            private RangeQuery(String fieldName, long lowerValue, long upperValue) {
                super(fieldName);
                this.lowerValue = lowerValue;
                this.upperValue = upperValue;
            }

            @Override
            protected boolean matches() {
                for (int i = 0; i < count; i++) {
                    if (lowerValue <= values[i] && values[i] <= upperValue) {
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
                return lowerValue == other.lowerValue && upperValue == other.upperValue;
            }
        }

        private class TermQuery extends AbstractRuntimeQuery {
            private final long term;

            private TermQuery(String fieldName, long term) {
                super(fieldName);
                this.term = term;
            }

            @Override
            protected boolean matches() {
                for (int i = 0; i < count; i++) {
                    if (term == values[i]) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void visit(QueryVisitor visitor) {
                visitor.consumeTerms(this, new Term(fieldName, Long.toString(term)));
            }

            @Override
            protected String bareToString() {
                return Long.toString(term);
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
                return term == other.term;
            }
        }

        private class TermsQuery extends AbstractRuntimeQuery {
            private final long[] terms;

            private TermsQuery(String fieldName, long[] terms) {
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
                for (long term : terms) {
                    visitor.consumeTerms(this, new Term(fieldName, Long.toString(term)));
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
    }
}
