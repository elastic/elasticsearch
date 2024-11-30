/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.search;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;

public class SparseVectorQuery extends Query {
    private final String fieldName;
    private final Query termsQuery;

    public SparseVectorQuery(String fieldName, Query termsQuery) {
        this.fieldName = fieldName;
        this.termsQuery = termsQuery;
    }

    public Query getTermsQuery() {
        return termsQuery;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        var rewrite = termsQuery.rewrite(indexSearcher);
        if (rewrite != termsQuery) {
            return new SparseVectorQuery(fieldName, rewrite);
        }
        return this;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return termsQuery.createWeight(searcher, scoreMode, boost);
    }

    @Override
    public String toString(String field) {
        return termsQuery.toString(field);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName)) {
            termsQuery.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        SparseVectorQuery that = (SparseVectorQuery) obj;
        return fieldName.equals(that.fieldName) && termsQuery.equals(that.termsQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, termsQuery);
    }
}
