/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.lucene.queries;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanWeight;
import org.apache.lucene.queries.spans.Spans;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * A {@link SpanQuery} that matches no documents.
 */
public class SpanMatchNoDocsQuery extends SpanQuery {
    private final String field;
    private final String reason;

    public SpanMatchNoDocsQuery(String field, String reason) {
        this.field = field;
        this.reason = reason;
    }

    @Override
    public String getField() {
        return field;
    }

    @Override
    public String toString(String field) {
        return "SpanMatchNoDocsQuery(\"" + reason + "\")";
    }

    @Override
    public boolean equals(Object o) {
        return sameClassAs(o);
    }

    @Override
    public int hashCode() {
        return classHash();
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public SpanWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new SpanWeight(this, searcher, Collections.emptyMap(), boost) {
            @Override
            public void extractTermStates(Map<Term, TermStates> contexts) {}

            @Override
            public Spans getSpans(LeafReaderContext ctx, Postings requiredPostings) {
                return null;
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }
        };
    }
}
