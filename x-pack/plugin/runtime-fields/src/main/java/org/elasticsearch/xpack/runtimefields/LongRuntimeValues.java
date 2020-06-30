/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.CheckedFunction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

public final class LongRuntimeValues extends AbstractRuntimeValues<LongRuntimeValues.SharedValues> {
    @FunctionalInterface
    public interface NewLeafLoader {
        IntConsumer leafLoader(LeafReaderContext ctx, LongConsumer sync) throws IOException;
    }

    private final NewLeafLoader newLeafLoader;

    public LongRuntimeValues(NewLeafLoader newLeafLoader) {
        this.newLeafLoader = newLeafLoader;
    }

    public CheckedFunction<LeafReaderContext, SortedNumericDocValues, IOException> docValues() {
        return unstarted().docValues();
    }

    public Query termQuery(String fieldName, long value) {
        return unstarted().new TermQuery(fieldName, value);
    }

    @Override
    protected SharedValues newSharedValues() {
        return new SharedValues();
    }

    protected class SharedValues extends AbstractRuntimeValues<SharedValues>.SharedValues {
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

        private class TermQuery extends Query {
            private final String fieldName;
            private final long term;

            private TermQuery(String fieldName, long term) {
                this.fieldName = fieldName;
                this.term = term;
            }

            @Override
            public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
                return new ConstantScoreWeight(this, boost) {
                    @Override
                    public boolean isCacheable(LeafReaderContext ctx) {
                        return false; // scripts aren't really cacheable at this point
                    }

                    @Override
                    public Scorer scorer(LeafReaderContext ctx) throws IOException {
                        IntConsumer leafCursor = leafCursor(ctx);
                        DocIdSetIterator approximation = DocIdSetIterator.all(ctx.reader().maxDoc());
                        TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {
                            @Override
                            public boolean matches() throws IOException {
                                leafCursor.accept(approximation.docID());
                                for (int i = 0; i < count; i++) {
                                    if (term == values[i]) {
                                        return true;
                                    }
                                }
                                return false;
                            }

                            @Override
                            public float matchCost() {
                                // TODO we have no idea what this should be and no real way to get one
                                return 1000f;
                            }
                        };
                        return new ConstantScoreScorer(this, score(), scoreMode, twoPhase);
                    }

                    @Override
                    public void extractTerms(Set<Term> terms) {
                        terms.add(new Term(fieldName, Long.toString(term)));
                    }
                };
            }

            @Override
            public void visit(QueryVisitor visitor) {
                visitor.consumeTerms(this, new Term(fieldName, Long.toString(term)));
            }

            @Override
            public String toString(String field) {
                if (fieldName.contentEquals(field)) {
                    return Long.toString(term);
                }
                return fieldName + ":" + term;
            }

            @Override
            public int hashCode() {
                return Objects.hash(fieldName, term);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null || getClass() != obj.getClass()) {
                    return false;
                }
                TermQuery other = (TermQuery) obj;
                return fieldName.equals(other.fieldName) && term == other.term;
            }
        }
    }
}
