/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;
import java.util.function.IntConsumer;

/**
 * Abstract base for implementing doc values and queries against values
 * calculated at runtime. The tricky thing about this is that we'd like to
 * calculate the values as few times as possible in case the calculation is
 * expensive, <strong>but</strong> some of the APIs that we rely on to
 * calculate the values like {@link SortedNumericDocValues#advanceExact(int)}
 * are "forwards only".
 * <p>
 * We solve this in the same way that big cities handle public transportation:
 * with a bus! In our case, the bus is subclasses of {@link SharedValues}.
 * Queries and doc values are implemented calling {@link #unstarted()} to get
 * the {@linkplain SharedValues} that has yet to start iterating. That way
 * many queries can share the same underlying {@linkplain SharedValues}
 * instance, only calculating the values for a document once. If other code
 * needs to iterate the values after the first iteration has started then
 * it'll get a new {@linkplain SharedValues} from {@linkplain #unstarted},
 * this "leaving on a different bus".
 *
 * @param <SV> the subtype of {@link SharedValues} needed by the subclass
 */
public abstract class AbstractRuntimeValues<SV extends AbstractRuntimeValues<SV>.SharedValues> {
    private SV unstarted;

    protected final SV unstarted() {
        if (unstarted == null) {
            unstarted = newSharedValues();
        }
        return unstarted;
    }

    protected abstract SV newSharedValues();

    protected abstract class SharedValues {
        protected int count;
        private boolean sort;

        private int lastDocBase = -1;
        private IntConsumer lastLeafCursor;
        private int docId = -1;
        private int maxDoc;

        protected final IntConsumer leafCursor(LeafReaderContext ctx) throws IOException {
            if (lastDocBase != ctx.docBase) {
                if (lastDocBase == -1) {
                    // Now that we're started future iterations can't share these values.
                    unstarted = null;
                }
                lastDocBase = ctx.docBase;
                IntConsumer leafLoader = newLeafLoader(ctx);
                docId = -1;
                maxDoc = ctx.reader().maxDoc();
                lastLeafCursor = new IntConsumer() {
                    @Override
                    public void accept(int targetDocId) {
                        if (docId == targetDocId) {
                            return;
                        }
                        docId = targetDocId;
                        count = 0;
                        leafLoader.accept(targetDocId);
                        if (sort) {
                            sort();
                        }
                    }
                };
            }
            return lastLeafCursor;
        }

        protected final void alwaysSortResults() {
            sort = true;
        }

        protected final int docId() {
            return docId;
        }

        protected final int maxDoc() {
            return maxDoc;
        }

        protected abstract IntConsumer newLeafLoader(LeafReaderContext ctx) throws IOException;

        protected abstract void sort();

        protected abstract class AbstractRuntimeQuery extends Query {
            protected final String fieldName;

            protected AbstractRuntimeQuery(String fieldName) {
                this.fieldName = fieldName;
            }

            @Override
            public final Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
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
                                return AbstractRuntimeQuery.this.matches();
                            }

                            @Override
                            public float matchCost() {
                                // TODO we don't have a good way of estimating the complexity of the script so we just go with 9000
                                return approximation().cost() * 9000f;
                            }
                        };
                        return new ConstantScoreScorer(this, score(), scoreMode, twoPhase);
                    }
                };
            }

            protected abstract boolean matches();

            @Override
            public final String toString(String field) {
                if (fieldName.contentEquals(field)) {
                    return bareToString();
                }
                return fieldName + ":" + bareToString();
            }

            protected abstract String bareToString();

            @Override
            public int hashCode() {
                return Objects.hash(fieldName);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null || getClass() != obj.getClass()) {
                    return false;
                }
                @SuppressWarnings("unchecked")
                AbstractRuntimeQuery other = (AbstractRuntimeQuery) obj;
                return fieldName.equals(other.fieldName);
            }
        }
    }
}
