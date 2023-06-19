/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

public final class MatchAllDocumentQueryWrapper extends Query {
    private final String pattern;
    private final MatchAllDocsQuery matchAllDocsQuery;

    public MatchAllDocumentQueryWrapper(String pattern) {
        this.matchAllDocsQuery = new MatchAllDocsQuery();
        this.pattern = pattern;
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return matchAllDocsQuery.createWeight(searcher, scoreMode, boost);
    }

    @Override
    public String toString(String field) {
        return matchAllDocsQuery.toString();
    }

    @Override
    public boolean equals(Object o) {
        return matchAllDocsQuery.equals(o);
    }

    @Override
    public int hashCode() {
        return matchAllDocsQuery.hashCode();
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(matchAllDocsQuery);
    }
}
