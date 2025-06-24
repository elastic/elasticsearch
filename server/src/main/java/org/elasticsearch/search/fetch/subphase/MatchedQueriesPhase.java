/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class MatchedQueriesPhase implements FetchSubPhase {
    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) throws IOException {
        Map<String, Query> namedQueries = new HashMap<>();
        if (context.parsedQuery() != null) {
            namedQueries.putAll(context.parsedQuery().namedFilters());
        }
        if (context.parsedPostFilter() != null) {
            namedQueries.putAll(context.parsedPostFilter().namedFilters());
        }
        if (namedQueries.isEmpty()) {
            return null;
        }
        Map<String, Weight> weights = new HashMap<>();
        for (Map.Entry<String, Query> entry : namedQueries.entrySet()) {
            weights.put(
                entry.getKey(),
                context.searcher().createWeight(context.searcher().rewrite(entry.getValue()), ScoreMode.COMPLETE_NO_SCORES, 1)
            );
        }
        return new FetchSubPhaseProcessor() {

            final Map<String, ScorerAndIterator> matchingIterators = new HashMap<>();

            @Override
            public void setNextReader(LeafReaderContext readerContext) throws IOException {
                matchingIterators.clear();
                for (Map.Entry<String, Weight> entry : weights.entrySet()) {
                    ScorerSupplier ss = entry.getValue().scorerSupplier(readerContext);
                    if (ss != null) {
                        Scorer scorer = ss.get(0L);
                        if (scorer != null) {
                            final TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
                            final DocIdSetIterator iterator;
                            if (twoPhase == null) {
                                iterator = scorer.iterator();
                            } else {
                                iterator = twoPhase.approximation();
                            }
                            matchingIterators.put(entry.getKey(), new ScorerAndIterator(scorer, iterator, twoPhase));
                        }
                    }
                }
            }

            @Override
            public void process(HitContext hitContext) throws IOException{
                List<String> matches = new ArrayList<>();
                int doc = hitContext.docId();
                for (Map.Entry<String, ScorerAndIterator> entry : matchingIterators.entrySet()) {
                    ScorerAndIterator query = entry.getValue();
                    if (query.approximation.docID() < doc) {
                        query.approximation.advance(doc);
                    }
                    if (query.approximation.docID() == doc && (query.twoPhase == null || query.twoPhase.matches())) {
                        matches.add(entry.getKey());
                    }
                }
                hitContext.hit().matchedQueries(matches.toArray(new String[0]));
            }
        };
    }
    public class ScorerAndIterator {
        private final Scorer scorer;
        private final DocIdSetIterator approximation;
        private final TwoPhaseIterator twoPhase;
        public ScorerAndIterator(Scorer scorer, DocIdSetIterator approximation, TwoPhaseIterator twoPhase) {
            this.scorer = scorer;
            this.approximation = approximation;
            this.twoPhase = twoPhase;
        }
        public Scorer getScorer() {
            return scorer;
        }
        public DocIdSetIterator getApproximation() {
            return approximation;
        }
        public TwoPhaseIterator getTwoPhase() {
            return twoPhase;
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ScorerAndIterator that = (ScorerAndIterator) o;
            if (!scorer.equals(that.scorer)) return false;
            if (!approximation.equals(that.approximation)) return false;
            return twoPhase.equals(that.twoPhase);
        }
        @Override
        public int hashCode() {
            int result = scorer.hashCode();
            result = 31 * result + approximation.hashCode();
            result = 31 * result + twoPhase.hashCode();
            return result;
        }
        @Override
        public String toString() {
            return "ScorerAndIterator{" +
                "scorer=" + scorer +
                ", approximation=" + approximation +
                ", twoPhase=" + twoPhase +
                '}';
        }
    }
}
