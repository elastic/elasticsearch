/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.ValueSourceScorer;
import org.apache.lucene.queries.function.valuesource.VectorSimilarityFunction;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Map;

/**
 * A query that retrieves all documents with a {@link ValueSource}. Ensures that the document
 * exists within the score function.
 */
public class ExactKnnQuery extends Query {
    final VectorSimilarityFunction func;

    public ExactKnnQuery(VectorSimilarityFunction func) {
        this.func = func;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new BruteForceWeight(searcher, boost);
    }

    VectorSimilarityFunction getFunc() {
        return func;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    /** Prints a user-readable version of this query. */
    @Override
    public String toString(String field) {
        return func.toString();
    }

    /** Returns true if <code>o</code> is equal to this. */
    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) && func.equals(((ExactKnnQuery) other).func);
    }

    @Override
    public int hashCode() {
        return classHash() ^ func.hashCode();
    }

    class BruteForceWeight extends Weight {
        protected final float boost;
        protected final Map<Object, Object> context;

        BruteForceWeight(IndexSearcher searcher, float boost) throws IOException {
            super(ExactKnnQuery.this);
            this.context = ValueSource.newContext(searcher);
            func.createWeight(context, searcher);
            this.boost = boost;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            FunctionValues values = func.getValues(this.context, context);
            if (values.exists(doc) == false) {
                return Explanation.noMatch("no matching value");
            }
            float sc = values.floatVal(doc);
            return Explanation.match(sc, "BruteForceKnnQuery(" + func + "), product of:", values.explain(doc));
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            FunctionValues values = func.getValues(this.context, context);
            return new ValueSourceScorer(this, context, values) {
                @Override
                public boolean matches(int doc) throws IOException {
                    return values.exists(doc);
                }

                @Override
                public float matchCost() {
                    return values.cost();
                }
            };
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return false;
        }
    }

}
