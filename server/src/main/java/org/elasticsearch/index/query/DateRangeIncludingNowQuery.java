/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

import java.io.IOException;
import java.util.Objects;

/**
 * A simple wrapper class that indicates that the wrapped query has made use of NOW
 * when parsing its datemath.  Useful for preprocessors such as the percolator that
 * need to know when not to extract dates from the query.
 */
public class DateRangeIncludingNowQuery extends Query {

    private final Query in;

    public DateRangeIncludingNowQuery(Query in) {
        this.in = in;
    }

    public Query getQuery() {
        return in;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        return in;
    }

    @Override
    public String toString(String field) {
        return "DateRangeIncludingNowQuery(" + in + ")";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        in.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (sameClassAs(o) == false) return false;
        DateRangeIncludingNowQuery that = (DateRangeIncludingNowQuery) o;
        return Objects.equals(in, that.in);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), in);
    }
}
