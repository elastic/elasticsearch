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
package org.elasticsearch.index.query.compound;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Scorer;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Set;


public class CompoundQuery extends Query {
    private final Query[] queries;
    private final CombineMode combineMode;

    public CompoundQuery(Query[] queries, CombineMode combineMode) {
        this.queries = queries;
        this.combineMode = combineMode;
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
            if (queries[i] != rewrittenQuery) {
                rewritten = true;
                rewrittenQueries[i] = rewrittenQuery;
            } else {
                rewrittenQueries[i] = queries[i];
            }
        }
        if (rewritten) {
            return new CompoundQuery(queries, combineMode);
        }
        return super.rewrite(reader);
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
        return new CompoundWeight(this, combineMode, searcher, scoreMode, boost);
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("compound (combine_mode:" + combineMode.toString());
        sb.append(", queries: [");
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
        CompoundQuery other = (CompoundQuery) o;
        return Arrays.equals(this.queries, other.queries) && Objects.equals(this.combineMode, other.combineMode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(queries), combineMode);
    }

    public enum CombineMode implements Writeable {
        FIRST, AVG, MAX, SUM, MIN, MULTIPLY;

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        public static CombineMode readFromStream(StreamInput in) throws IOException {
            return in.readEnum(CombineMode.class);
        }

        public static CombineMode fromString(String combineMode) {
            return valueOf(combineMode.toUpperCase(Locale.ROOT));
        }
    }

    static final class CompoundWeight extends Weight {
        private final Weight[] queryWeights;
        private final ScoreMode scoreMode;
        private final CombineMode combineMode;

        CompoundWeight(CompoundQuery compoundQuery, CombineMode combineMode, IndexSearcher searcher,
                ScoreMode scoreMode, float boost) throws IOException {
            super(compoundQuery);
            Query[] queries = compoundQuery.getQueries();
            this.queryWeights = new Weight[queries.length];
            this.combineMode = combineMode;
            this.scoreMode = scoreMode;
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
            List<Matches> matches = new ArrayList<>();
            for (Weight w: queryWeights) {
                Matches m = w.matches(context, doc);
                if (m != null) {
                    matches.add(m);
                    if (combineMode == CombineMode.FIRST) break;
                }
            }
            return MatchesUtils.fromSubMatches(matches);
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
            return new CompoundScorerSupplier(this, scoreMode, scorerSuppliers, combineMode);
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
            boolean match = false;
            List<Explanation> subExplanations = new ArrayList<>();
            for (Weight w : queryWeights) {
                Explanation e = w.explain(context, doc);
                if (e.isMatch()) {
                    match = true;
                    subExplanations.add(e);
                    if (combineMode == CombineMode.FIRST) break;
                }
            }
            if (match) {
                Scorer scorer = scorer(context);
                int advanced = scorer.iterator().advance(doc);
                assert advanced == doc;
                String desc;
                switch (combineMode) {
                    case FIRST:
                        desc = "first of:";
                        break;
                    case MAX:
                        desc = "max of:";
                        break;
                    case MIN:
                        desc = "min of:";
                        break;
                    case MULTIPLY:
                        desc = "multiplication of:";
                        break;
                    case SUM:
                        desc = "sum of:";
                        break;
                    default: //average
                        desc = "average of";
                }
                return Explanation.match(scorer.score(), desc, subExplanations);
            } else {
                return Explanation.noMatch("No matching clause");
            }
        }
    }

    static final class CompoundScorerSupplier extends ScorerSupplier {
        private final Weight weight;
        private final ScoreMode scoreMode;
        private final List<ScorerSupplier> scorerSuppliers;
        private final CombineMode combineMode;
        private long cost = -1;

        CompoundScorerSupplier(Weight weight, ScoreMode scoreMode, List<ScorerSupplier> scorerSuppliers, CombineMode combineMode) {
            this.weight = weight;
            this.scoreMode = scoreMode;
            this.scorerSuppliers = scorerSuppliers;
            this.combineMode = combineMode;
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
            return new DisjunctionCombineScorer(weight, scorers, combineMode, scoreMode);
        }
    }
}


