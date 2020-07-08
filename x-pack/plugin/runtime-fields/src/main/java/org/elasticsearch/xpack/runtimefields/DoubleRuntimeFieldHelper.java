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
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;

/**
 * Manages the creation of doc values and queries for {@code double} fields.
 */
public final class DoubleRuntimeFieldHelper {
    @FunctionalInterface
    public interface NewLeafLoader {
        IntConsumer leafLoader(LeafReaderContext ctx, DoubleConsumer sync) throws IOException;
    }

    private final NewLeafLoader newLeafLoader;

    public DoubleRuntimeFieldHelper(NewLeafLoader newLeafLoader) {
        this.newLeafLoader = newLeafLoader;
    }

    public CheckedFunction<LeafReaderContext, SortedNumericDoubleValues, IOException> docValues() {
        return new Values().docValues();
    }

    public Query existsQuery(String fieldName) {
        return new Values().new ExistsQuery(fieldName);
    }

    public Query rangeQuery(String fieldName, double lowerValue, double upperValue) {
        return new Values().new RangeQuery(fieldName, lowerValue, upperValue);
    }

    public Query termQuery(String fieldName, double value) {
        return new Values().new TermQuery(fieldName, value);
    }

    public Query termsQuery(String fieldName, double... value) {
        return new Values().new TermsQuery(fieldName, value);
    }

    private class Values extends AbstractRuntimeValues {
        private double[] values = new double[1];

        @Override
        protected IntConsumer newLeafLoader(LeafReaderContext ctx) throws IOException {
            return newLeafLoader.leafLoader(ctx, this::add);
        }

        private void add(double value) {
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

        private CheckedFunction<LeafReaderContext, SortedNumericDoubleValues, IOException> docValues() {
            alwaysSortResults();
            return DocValues::new;
        }

        private class DocValues extends SortedNumericDoubleValues {
            private final IntConsumer leafCursor;
            private int next;

            DocValues(LeafReaderContext ctx) throws IOException {
                leafCursor = leafCursor(ctx);
            }

            @Override
            public double nextValue() throws IOException {
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
            private final double lowerValue;
            private final double upperValue;

            private RangeQuery(String fieldName, double lowerValue, double upperValue) {
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
            private final double term;

            private TermQuery(String fieldName, double term) {
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
                visitor.consumeTerms(this, new Term(fieldName, Double.toString(term)));
            }

            @Override
            protected String bareToString() {
                return Double.toString(term);
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
            private final double[] terms;

            private TermsQuery(String fieldName, double[] terms) {
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
                for (double term : terms) {
                    visitor.consumeTerms(this, new Term(fieldName, Double.toString(term)));
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
