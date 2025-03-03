/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.rescore.RescoreContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
        for (RescoreContext rescoreContext : context.rescore()) {
            for (ParsedQuery parsedQuery : rescoreContext.getParsedQueries()) {
                namedQueries.putAll(parsedQuery.namedFilters());
            }
        }
        if (namedQueries.isEmpty()) {
            return null;
        }
        Map<String, Weight> weights = new HashMap<>();
        for (Map.Entry<String, Query> entry : namedQueries.entrySet()) {
            weights.put(
                entry.getKey(),
                context.searcher().createWeight(context.searcher().rewrite(entry.getValue()), ScoreMode.COMPLETE, 1)
            );
        }
        return new FetchSubPhaseProcessor() {
            record ScorerAndIterator(Scorer scorer, DocIdSetIterator approximation, TwoPhaseIterator twoPhase) {}

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
            public void process(HitContext hitContext) throws IOException {
                Map<String, Float> matches = new LinkedHashMap<>();
                int doc = hitContext.docId();
                for (Map.Entry<String, ScorerAndIterator> entry : matchingIterators.entrySet()) {
                    ScorerAndIterator query = entry.getValue();
                    if (query.approximation.docID() < doc) {
                        query.approximation.advance(doc);
                    }
                    if (query.approximation.docID() == doc && (query.twoPhase == null || query.twoPhase.matches())) {
                        matches.put(entry.getKey(), query.scorer.score());
                    }
                }
                hitContext.hit().matchedQueries(matches);
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NO_REQUIREMENTS;
            }
        };
    }
}
