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
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.search.collapse.CollapseContext;
import org.elasticsearch.search.fetch.subphase.FetchDocValuesContext;
import org.elasticsearch.search.fetch.subphase.FetchFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsContext;
import org.elasticsearch.search.fetch.subphase.highlight.SearchHighlightContext;
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

    public boolean hasOnlySuggest() {
        return searchContext.hasOnlySuggest();
    }

    public ContextIndexSearcher searcher() {
        return searchContext.searcher();
    }

    public MapperService mapperService() {
        return searchContext.mapperService();
    }

    public IndexSettings getIndexSettings() {
        return mapperService().getIndexSettings();
    }

    public IndexFieldData<?> getForField(MappedFieldType fieldType) {
        return searchContext.getForField(fieldType);
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

    public FetchSourceContext fetchSourceContext() {
        return searchContext.fetchSourceContext();
    }

    public boolean explain() {
        return searchContext.explain();
    }

    public List<RescoreContext> rescore() {
        return searchContext.rescore();
    }

    public boolean seqNoAndPrimaryTerm() {
        return searchContext.seqNoAndPrimaryTerm();
    }

    public CollapseContext collapse() {
        return searchContext.collapse();
    }

    public FetchDocValuesContext docValuesContext() {
        return searchContext.docValuesContext();
    }

    public SearchHighlightContext highlight() {
        return searchContext.highlight();
    }

    public boolean fetchScores() {
        return getFetchSize() > 0 && searchContext.sort() != null && searchContext.trackScores();
    }

    public InnerHitsContext innerHits() {
        return searchContext.innerHits();
    }

    public boolean version() {
        return searchContext.version();
    }

    public StoredFieldsContext storedFieldsContext() {
        return searchContext.storedFieldsContext();
    }

    public FetchFieldsContext fetchFieldsContext() {
        return searchContext.fetchFieldsContext();
    }

    public ScriptFieldsContext scriptFields() {
        return searchContext.scriptFields();
    }

    public SearchExtBuilder getSearchExt(String name) {
        return searchContext.getSearchExt(name);
    }
}
