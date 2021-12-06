/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;

public class KnnVectorFieldExistsQuery extends Query {

    private final String field;

    /** Create a query that will match documents which have a value for the given {@code field}. */
    public KnnVectorFieldExistsQuery(String field) {
        this.field = Objects.requireNonNull(field);
    }

    public String getField() {
        return field;
    }

    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) && field.equals(((KnnVectorFieldExistsQuery) other).field);
    }

    @Override
    public int hashCode() {
        return 31 * classHash() + field.hashCode();
    }

    @Override
    public String toString(String field) {
        return "KnnVectorFieldExistsQuery [field=" + this.field + "]";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                DocIdSetIterator iterator = context.reader().getVectorValues(field);
                if (iterator == null) {
                    return null;
                }
                return new ConstantScoreScorer(this, score(), scoreMode, iterator);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }
        };
    }
}
