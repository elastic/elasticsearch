/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rescore.RescoreContext;

import java.util.List;

public class FetchContext {

    public FetchContext(SearchContext searchContext) {
        this.searchContext = searchContext;
    }

    public static FetchContext fromSearchContext(SearchContext context) {
        return new FetchContext(context);
    }

    private final SearchContext searchContext;

    public String getIndexName() {
        return searchContext.indexShard().shardId().getIndexName();
    }

    public int getFetchSize() {
        return searchContext.docIdsToLoadSize();
    }

    public ContextIndexSearcher searcher() {
        return searchContext.searcher();
    }

    public Query query() {
        return searchContext.query();
    }

    public ParsedQuery parsedQuery() {
        return searchContext.parsedQuery();
    }

    public ParsedQuery parsedPostFilter() {
        return searchContext.parsedPostFilter();
    }

    public boolean sourceRequested() {
        return searchContext.sourceRequested();
    }

    public List<RescoreContext> rescore() {
        return searchContext.rescore();
    }

    public FetchSourceContext fetchSourceContext() {
        return searchContext.fetchSourceContext();
    }

    public boolean hasOnlySuggest() {
        return searchContext.hasOnlySuggest();
    }

    public boolean explain() {
        return searchContext.explain();
    }
}
