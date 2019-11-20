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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class NamedSpanQuery extends SpanQuery {

    private final String name;
    private final SpanQuery in;

    public NamedSpanQuery(String name, SpanQuery in) {
        this.name = name;
        this.in = in;
    }

    public String getName() {
        return name;
    }

    public SpanQuery getQuery() {
        return in;
    }

    @Override
    public String getField() {
        return in.getField();
    }

    @Override
    public String toString(String field) {
        return "NamedSpanQuery(" + name + "," + in.toString(field) + ")";
    }

    @Override
    public SpanWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        SpanWeight w = in.createWeight(searcher, scoreMode, boost);
        return new SpanWeight(this, searcher, scoreMode.needsScores() ? getTermStates(w) : null, boost) {
            @Override
            public void extractTermStates(Map<Term, TermStates> contexts) {
                w.extractTermStates(contexts);
            }

            @Override
            public Spans getSpans(LeafReaderContext ctx, Postings requiredPostings) throws IOException {
                return w.getSpans(ctx, requiredPostings);
            }

            @Override
            public void extractTerms(Set<Term> terms) {
                w.extractTerms(terms);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return w.isCacheable(ctx);
            }

            @Override
            public Matches matches(LeafReaderContext context, int doc) throws IOException {
                Matches m = w.matches(context, doc);
                if (m == null) {
                    return null;
                }
                return new NamedQuery.NamedMatches(name, m);
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NamedSpanQuery that = (NamedSpanQuery) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(in, that.in);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        in.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, in);
    }
}
