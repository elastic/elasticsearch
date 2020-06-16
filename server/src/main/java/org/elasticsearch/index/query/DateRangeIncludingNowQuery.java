/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        if (o == null || getClass() != o.getClass()) return false;
        DateRangeIncludingNowQuery that = (DateRangeIncludingNowQuery) o;
        return Objects.equals(in, that.in);
    }

    @Override
    public int hashCode() {
        return Objects.hash(in);
    }
}
