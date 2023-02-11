/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DisjunctionDISIApproximation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class RRFRankQuery extends Query {

    public static class RRFRankScore extends Scorer {

        private final DisiPriorityQueue scorers;

        public RRFRankScore(Weight weight, List<Scorer> scorers) {
            super(weight);
            this.scorers = new DisiPriorityQueue(scorers.size());
            for (Scorer scorer : scorers) {
                this.scorers.add(new DisiWrapper(scorer));
            }
        }

        @Override
        public DocIdSetIterator iterator() {
            return new DisjunctionDISIApproximation(scorers);
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
            return Float.MAX_VALUE;
        }

        @Override
        public float score() throws IOException {
            double score = 0;

            for (DisiWrapper wrapper = scorers.topList(); wrapper != null; wrapper = wrapper.next) {
                score += wrapper.scorer.score();
            }

            return (float) score;
        }

        @Override
        public int docID() {
            return scorers.top().doc;
        }
    }

    public static class RRFRankWeight extends Weight {

        private final List<Consumer<Scorer>> consumers;
        private final List<Weight> weights = new ArrayList<>();

        public RRFRankWeight(
            List<Consumer<Scorer>> consumers,
            RRFRankQuery RRFRankQuery,
            IndexSearcher searcher,
            ScoreMode scoreMode,
            float boost
        ) throws IOException {
            super(RRFRankQuery);
            this.consumers = consumers;

            for (Query query : RRFRankQuery.queries) {
                weights.add(query.createWeight(searcher, scoreMode, boost));
            }
        }

        @Override
        public Matches matches(LeafReaderContext context, int doc) throws IOException {
            List<Matches> matches = new ArrayList<>();
            for (Weight weight : weights) {
                matches.add(weight.matches(context, doc));
            }
            return MatchesUtils.fromSubMatches(matches);
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            List<Scorer> scorers = new ArrayList<>();
            List<Explanation> explanations = new ArrayList<>();
            for (Weight weight : weights) {
                scorers.add(weight.scorer(context));
                explanations.add(weight.explain(context, doc));
            }
            Scorer scorer = new RRFRankScore(this, scorers);
            scorer.iterator().advance(doc);
            if (scorer.docID() == doc) {
                return Explanation.match(scorer.score(), "sum of: ", explanations);
            } else {
                return Explanation.noMatch("child scorers: ", explanations);
            }
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            List<Scorer> scorers = new ArrayList<>();
            int index = 0;
            for (Weight weight : weights) {
                Scorer scorer = weight.scorer(context);
                consumers.get(index++).accept(scorer);
                scorers.add(scorer);
            }
            return new RRFRankScore(this, scorers);
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            return super.scorerSupplier(context);
        }

        @Override
        public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
            return super.bulkScorer(context);
        }

        @Override
        public int count(LeafReaderContext context) throws IOException {
            return super.count(context);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            for (Weight weight : weights) {
                if (weight.isCacheable(ctx) == false) {
                    return false;
                }
            }

            return true;
        }
    }

    private final List<Consumer<Scorer>> consumers;
    private final List<Query> queries;

    public RRFRankQuery(List<Consumer<Scorer>> consumers, List<Query> queries) {
        this.consumers = Collections.unmodifiableList(Objects.requireNonNull(consumers));
        this.queries = Collections.unmodifiableList(Objects.requireNonNull(queries));
    }

    public List<Consumer<Scorer>> getConsumers() {
        return consumers;
    }

    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new RRFRankWeight(consumers, this, searcher, scoreMode, boost);
    }

    public Query rewrite(IndexReader reader) throws IOException {
        boolean rewritten = false;

        List<Query> rewrittenQueries = new ArrayList<>();
        for (Query query : queries) {
            Query rewrittenQuery = query.rewrite(reader);
            rewritten |= rewrittenQuery != query;
            rewrittenQueries.add(rewrittenQuery);
        }

        return rewritten ? new RRFRankQuery(consumers, rewrittenQueries) : this;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        for (Query query : queries) {
            query.visit(visitor);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RRFRankQuery RRFRankQuery = (RRFRankQuery) o;
        return Objects.equals(consumers, RRFRankQuery.consumers) && Objects.equals(queries, RRFRankQuery.queries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumers, queries);
    }

    @Override
    public String toString(String string) {
        return "RankQuery{" + "consumers=" + consumers + ", queries=" + queries + '}';
    }
}
