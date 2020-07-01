/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

/**
 * Forces wrapped queries to score documents one at a time which is important
 * for runtime queries so they can more in lock step with one another.
 * <p>
 * Inspired by the ForceNoBulkScoringQuery in Lucene's monitor project.
 */
class ForceNoBulkScoringQuery extends Query {
    private final Query delegate;

    ForceNoBulkScoringQuery(Query inner) {
        this.delegate = inner;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query rewritten = delegate.rewrite(reader);
        if (rewritten != delegate) {
            return new ForceNoBulkScoringQuery(rewritten);
        }
        return this;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        delegate.visit(visitor);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ForceNoBulkScoringQuery that = (ForceNoBulkScoringQuery) o;
        return Objects.equals(delegate, that.delegate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delegate);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        Weight innerWeight = delegate.createWeight(searcher, scoreMode, boost);
        return new Weight(this) {
            @Override
            public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
                /*
                 * Intentionally do not delegate this to the innerWeight so
                 * we don't use its BulkScorer. This is the magic that causes
                 * us to skip bulk scoring.
                 */
                return super.bulkScorer(context);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return innerWeight.isCacheable(ctx);
            }

            @Override
            public Explanation explain(LeafReaderContext leafReaderContext, int i) throws IOException {
                return innerWeight.explain(leafReaderContext, i);
            }

            @Override
            public Scorer scorer(LeafReaderContext leafReaderContext) throws IOException {
                return innerWeight.scorer(leafReaderContext);
            }

            @Override
            public Matches matches(LeafReaderContext context, int doc) throws IOException {
                return innerWeight.matches(context, doc);
            }

            @Override
            @Deprecated
            public void extractTerms(Set<Term> terms) {
                innerWeight.extractTerms(terms);
            }
        };
    }

    @Override
    public String toString(String s) {
        return "NoBulkScorer(" + delegate.toString(s) + ")";
    }
}
