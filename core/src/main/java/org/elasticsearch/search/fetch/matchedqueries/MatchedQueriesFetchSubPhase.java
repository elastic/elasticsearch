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
package org.elasticsearch.search.fetch.matchedqueries;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SearchContext.Lifetime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 *
 */
public class MatchedQueriesFetchSubPhase implements FetchSubPhase {

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        return emptyMap();
    }

    @Override
    public boolean hitsExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) {
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return !context.parsedQuery().namedFilters().isEmpty()
                || (context.parsedPostFilter() !=null && !context.parsedPostFilter().namedFilters().isEmpty());
    }

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) {
        List<String> matchedQueries = new ArrayList<>(2);

        try {
            addMatchedQueries(hitContext, context.parsedQuery().namedFilters(), matchedQueries);

            if (context.parsedPostFilter() != null) {
                addMatchedQueries(hitContext, context.parsedPostFilter().namedFilters(), matchedQueries);
            }
        } catch (IOException e) {
            throw ExceptionsHelper.convertToElastic(e);
        } finally {
            SearchContext.current().clearReleasables(Lifetime.COLLECTION);
        }

        hitContext.hit().matchedQueries(matchedQueries.toArray(new String[matchedQueries.size()]));
    }

    private void addMatchedQueries(HitContext hitContext, Map<String, Query> namedQueries, List<String> matchedQueries) throws IOException {
        for (Map.Entry<String, Query> entry : namedQueries.entrySet()) {
            String name = entry.getKey();
            Query filter = entry.getValue();

            final Weight weight = hitContext.topLevelSearcher().createNormalizedWeight(filter, false);
            final Scorer scorer = weight.scorer(hitContext.readerContext());
            if (scorer == null) {
                continue;
            }
            final TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
            if (twoPhase == null) {
                if (scorer.iterator().advance(hitContext.docId()) == hitContext.docId()) {
                    matchedQueries.add(name);
                }
            } else {
                if (twoPhase.approximation().advance(hitContext.docId()) == hitContext.docId() && twoPhase.matches()) {
                    matchedQueries.add(name);
                }
            }
        }
    }
}
