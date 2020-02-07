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
package org.elasticsearch.index.query.first;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Scorer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;


public class FirstQuery extends Query {
    private final Query[] queries;

    public FirstQuery(Query[] queries) {
        this.queries = queries;
    }

    public Query[] getQueries() {
        return queries;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        if (queries.length == 1) {
            return queries[0];
        }
        Query[] rewrittenQueries = new Query[queries.length];
        boolean rewritten = false;
        for (int i = 0; i < queries.length; i++) {
            Query rewrittenQuery = queries[i].rewrite(reader);
            rewrittenQueries[i] = rewrittenQuery;
            if (rewrittenQuery != queries[i]) {
                rewritten = true;
            }
        }
        if (rewritten) {
            return new FirstQuery(rewrittenQueries);
        }
        return this;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        QueryVisitor v = visitor.getSubVisitor(BooleanClause.Occur.SHOULD, this);
        for (Query q : queries) {
            q.visit(v);
        }
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new FirstWeight(this, searcher, scoreMode, boost);
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("first (queries: [");
        for (Query query : queries) {
            sb.append("{" + query.toString(field) + "}");
        }
        sb.append("])");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FirstQuery other = (FirstQuery) o;
        return Arrays.equals(this.queries, other.queries);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(queries);
    }

    static final class FirstWeight extends Weight {
        private final Weight[] queryWeights;

        FirstWeight(FirstQuery firstQuery, IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            super(firstQuery);
            Query[] queries = firstQuery.getQueries();
            this.queryWeights = new Weight[queries.length];
            for (int i = 0; i < queries.length; i++) {
                queryWeights[i] = searcher.createWeight(searcher.rewrite(queries[i]), scoreMode, boost);
            }
        }

        @Override
        public void extractTerms(Set<Term> terms) {
            for (Weight w: queryWeights) {
                w.extractTerms(terms);
            }
        }

        @Override
        public Matches matches(LeafReaderContext context, int doc) throws IOException {
            for (Weight w: queryWeights) {
                Matches m = w.matches(context, doc);
                if (m != null) {
                    return m; // return results from the 1st matching weight
                }
            }
            return null;
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            ScorerSupplier scorerSupplier = scorerSupplier(context);
            if (scorerSupplier == null) {
                return null;
            }
            return scorerSupplier.get(Long.MAX_VALUE);
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            List<ScorerSupplier> scorerSuppliers = new ArrayList<>();
            for (Weight w: queryWeights) {
                ScorerSupplier scorerSupplier = w.scorerSupplier(context);
                if (scorerSupplier != null) {
                    scorerSuppliers.add(scorerSupplier);
                }
            }
            if (scorerSuppliers.size() == 0) return null;
            if (scorerSuppliers.size() == 1) return scorerSuppliers.get(0);
            return new FirstScorerSupplier(this, scorerSuppliers);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            for (Weight w: queryWeights) {
                if (w.isCacheable(ctx) == false) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            for (Weight w : queryWeights) {
                Explanation e = w.explain(context, doc);
                if (e.isMatch()) {
                    Scorer scorer = scorer(context);
                    int advanced = scorer.iterator().advance(doc);
                    assert advanced == doc;
                    return Explanation.match(scorer.score(), "first of: ", e);
                }
            }
            return Explanation.noMatch("No matching clause");
        }
    }

    static final class FirstScorerSupplier extends ScorerSupplier {
        private final Weight weight;
        private final List<ScorerSupplier> scorerSuppliers;
        private long cost = -1;

        FirstScorerSupplier(Weight weight, List<ScorerSupplier> scorerSuppliers) {
            this.weight = weight;
            this.scorerSuppliers = scorerSuppliers;
        }

        @Override
        public long cost() {
            if (cost == -1) {
                cost = scorerSuppliers.stream().mapToLong(ScorerSupplier::cost).sum();
            }
            return cost;
        }

        @Override
        public Scorer get(long leadCost) throws IOException {
            leadCost = Math.min(leadCost, cost());
            final List<Scorer> scorers = new ArrayList<>();
            for (ScorerSupplier scorerSupplier : scorerSuppliers) {
                scorers.add(scorerSupplier.get(leadCost));
            };
            return new FirstScorer(weight, scorers);
        }
    }
}


