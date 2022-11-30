/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;

/**
 * A MatchNoDocsQuery that will not be recognised by Lucene's core rewriting rules,
 * useful for protecting the unified highlighter against rewriting. See
 * https://issues.apache.org/jira/browse/LUCENE-10454 for an explanation.
 */
public class NoRewriteMatchNoDocsQuery extends Query {

    private final String reason;

    public NoRewriteMatchNoDocsQuery(String reason) {
        this.reason = reason;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new Weight(this) {
            @Override
            public Explanation explain(LeafReaderContext context, int doc) {
                return Explanation.noMatch(reason);
            }

            @Override
            public Scorer scorer(LeafReaderContext context) {
                return null;
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }

            @Override
            public int count(LeafReaderContext context) {
                return 0;
            }
        };
    }

    @Override
    public String toString(String field) {
        return "NoRewriteMatchNoDocsQuery(" + reason + ")";
    }

    @Override
    public void visit(QueryVisitor visitor) {

    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof NoRewriteMatchNoDocsQuery q && Objects.equals(this.reason, q.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(reason);
    }
}
