/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.fetch.matchedqueries;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MatchedQueriesFetchSubPhase implements FetchSubPhase {

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of();
    }

    @Override
    public boolean hitsExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) throws ElasticSearchException {
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return !context.parsedQuery().namedFilters().isEmpty()
                || (context.parsedFilter() !=null && !context.parsedFilter().namedFilters().isEmpty());
    }

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) throws ElasticSearchException {
        List<String> matchedQueries = Lists.newArrayListWithCapacity(2);

        addMatchedQueries(hitContext, context.parsedQuery().namedFilters(), matchedQueries);

        if (context.parsedFilter() != null) {
            addMatchedQueries(hitContext, context.parsedFilter().namedFilters(), matchedQueries);
        }

        hitContext.hit().matchedQueries(matchedQueries.toArray(new String[matchedQueries.size()]));
    }

    private void addMatchedQueries(HitContext hitContext, ImmutableMap<String, Filter> namedQueriesAndFilters, List<String> matchedQueries) {
        for (Map.Entry<String, Filter> entry : namedQueriesAndFilters.entrySet()) {
            String name = entry.getKey();
            Filter filter = entry.getValue();
            try {
                DocIdSet docIdSet = filter.getDocIdSet(hitContext.readerContext(), null); // null is fine, since we filter by hitContext.docId()
                if (!DocIdSets.isEmpty(docIdSet)) {
                    Bits bits = docIdSet.bits();
                    if (bits != null) {
                        if (bits.get(hitContext.docId())) {
                            matchedQueries.add(name);
                        }
                    } else {
                        DocIdSetIterator iterator = docIdSet.iterator();
                        if (iterator != null) {
                            if (iterator.advance(hitContext.docId()) == hitContext.docId()) {
                                matchedQueries.add(name);
                            }
                        }
                    }
                }
            } catch (IOException e) {
                // ignore
            }
        }
    }
}
