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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class NamedQuery extends Query {

    public static class NamedMatches implements Matches {

        private final String name;
        private final Matches in;

        NamedMatches(String name, Matches in) {
            this.name = name;
            this.in = in;
        }

        public String getName() {
            return name;
        }

        @Override
        public MatchesIterator getMatches(String field) throws IOException {
            return in.getMatches(field);
        }

        @Override
        public Collection<Matches> getSubMatches() {
            return Collections.singleton(in);
        }

        @Override
        public Iterator<String> iterator() {
            return in.iterator();
        }
    }

    public static List<NamedMatches> findNamedMatches(Matches matches) {
        List<NamedMatches> namedMatches = new ArrayList<>();
        List<Matches> pending = new LinkedList<>();
        pending.add(matches);
        while (pending.size() > 0) {
            Matches m = pending.remove(0);
            if (m instanceof NamedMatches) {
                namedMatches.add((NamedMatches)m);
            }
            pending.addAll(m.getSubMatches());
        }
        return namedMatches;
    }

    private final String name;
    private final Query in;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NamedQuery that = (NamedQuery) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(in, that.in);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, in);
    }

    public NamedQuery(String name, Query in) {
        this.name = name;
        this.in = in;
    }

    public String getName() {
        return name;
    }

    public Query getQuery() {
        return in;
    }

    @Override
    public String toString(String field) {
        return "NamedQuery(" + name + "," + in.toString(field) + ")";
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new FilterWeight(in.createWeight(searcher, scoreMode, boost)) {
            @Override
            public Matches matches(LeafReaderContext context, int doc) throws IOException {
                Matches m = super.matches(context, doc);
                if (m == null) {
                    return null;
                }
                return new NamedMatches(name, m);
            }
        };
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query rewritten = in.rewrite(reader);
        if (rewritten != in) {
            return new NamedQuery(name, rewritten);
        }
        return this;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        in.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
    }
}
