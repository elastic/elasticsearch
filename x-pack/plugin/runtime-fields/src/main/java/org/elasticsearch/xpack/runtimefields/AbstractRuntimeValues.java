/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
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
 * calculated at runtime.
 */
public abstract class AbstractRuntimeValues {
    protected int count;
    private boolean sort;

    private int docId = -1;
    private int maxDoc;

    protected final IntConsumer leafCursor(LeafReaderContext ctx) throws IOException {
        IntConsumer leafLoader = newLeafLoader(ctx);
        docId = -1;
        maxDoc = ctx.reader().maxDoc();
        return new IntConsumer() {
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
            AbstractRuntimeQuery other = (AbstractRuntimeQuery) obj;
            return fieldName.equals(other.fieldName);
        }
    }
}
