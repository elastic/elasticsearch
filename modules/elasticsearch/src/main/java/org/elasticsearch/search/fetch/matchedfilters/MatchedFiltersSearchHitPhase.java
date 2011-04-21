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

package org.elasticsearch.search.fetch.matchedfilters;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.lucene.docset.DocSets;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.SearchHitPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class MatchedFiltersSearchHitPhase implements SearchHitPhase {

    @Override public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of();
    }

    @Override public boolean executionNeeded(SearchContext context) {
        return !context.parsedQuery().namedFilters().isEmpty();
    }

    @Override public void execute(SearchContext context, HitContext hitContext) throws ElasticSearchException {
        List<String> matchedFilters = Lists.newArrayListWithCapacity(2);
        for (Map.Entry<String, Filter> entry : context.parsedQuery().namedFilters().entrySet()) {
            String name = entry.getKey();
            Filter filter = entry.getValue();
            try {
                DocIdSet docIdSet = filter.getDocIdSet(hitContext.reader());
                if (docIdSet != null) {
                    DocSet docSet = DocSets.convert(hitContext.reader(), docIdSet);
                    if (docSet.get(hitContext.docId())) {
                        matchedFilters.add(name);
                    }
                }
            } catch (IOException e) {
                // ignore
            }
        }
        hitContext.hit().matchedFilters(matchedFilters.toArray(new String[matchedFilters.size()]));
    }
}
