/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
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

    public Query termQuery(String fieldName, String value) {
        return unstarted().new TermQuery(fieldName, value);
    }

    public Query prefixQuery(String fieldName, String value) {
        return unstarted().new PrefixQuery(fieldName, value);
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

        private class TermQuery extends Query {
            private final String fieldName;
            private final String term;

            private TermQuery(String fieldName, String term) {
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
                                    if (term.equals(values[i])) {
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
                        terms.add(new Term(fieldName, term));
                    }
                };
            }

            @Override
            public void visit(QueryVisitor visitor) {
                visitor.consumeTerms(this, new Term(fieldName, term));
            }

            @Override
            public String toString(String field) {
                if (fieldName.contentEquals(field)) {
                    return term;
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
                return fieldName.equals(other.fieldName) && term.equals(other.term);
            }
        }

        private class PrefixQuery extends Query {
            private final String fieldName;
            private final String prefix;

            private PrefixQuery(String fieldName, String prefix) {
                this.fieldName = fieldName;
                this.prefix = prefix;
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
                                    if (values[i].startsWith(prefix)) {
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
                        // TODO doing this is sort of difficult and maybe not needed.
                    }
                };
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
            public String toString(String field) {
                if (fieldName.contentEquals(field)) {
                    return prefix + "*";
                }
                return fieldName + ":" + prefix + "*";
            }

            @Override
            public int hashCode() {
                return Objects.hash(fieldName, prefix);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null || getClass() != obj.getClass()) {
                    return false;
                }
                PrefixQuery other = (PrefixQuery) obj;
                return fieldName.equals(other.fieldName) && prefix.equals(other.prefix);
            }
        }
    }
}
