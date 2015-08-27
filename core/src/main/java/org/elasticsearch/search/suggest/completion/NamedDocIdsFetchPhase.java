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

package org.elasticsearch.search.suggest.completion;

import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.FilteredSearchContext;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;

import java.util.Collections;
import java.util.Map;

/**
 * Fetches hits for named doc id sets
 * using <code>context.namedDocIdsToLoad()</code>
 */
public class NamedDocIdsFetchPhase implements FetchSubPhase {

    private FetchPhase fetchPhase;

    @Override
    public boolean hitsExecutionNeeded(SearchContext context) {
        return !(context instanceof NamedDocIdsFetchSearchContext) &&
                context.namedDocIdsToLoad() != null;
    }

    @Override
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) {
        assert !(context instanceof NamedDocIdsFetchSearchContext);
        NamedDocIdsFetchSearchContext fetchSearchContext = new NamedDocIdsFetchSearchContext(context);
        Map<String, int[]> namedDocIdsToLoad = context.namedDocIdsToLoad();
        for (Map.Entry<String, int[]> namedDocId : namedDocIdsToLoad.entrySet()) {
            fetchSearchContext.docIdsToLoad(namedDocId.getValue(), 0, namedDocId.getValue().length);
            fetchPhase.execute(fetchSearchContext);
            fetchSearchContext.fetchResult().namedHits(namedDocId.getKey(), fetchSearchContext.fetchResult().hits());
        }
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        return Collections.emptyMap();
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override
    public void hitExecute(SearchContext context, FetchSubPhase.HitContext hitContext) {
    }

    public void setFetchPhase(FetchPhase fetchPhase) {
        this.fetchPhase = fetchPhase;
    }

    private static class NamedDocIdsFetchSearchContext extends FilteredSearchContext {
        public NamedDocIdsFetchSearchContext(SearchContext in) {
            super(in);
        }
    }
}
