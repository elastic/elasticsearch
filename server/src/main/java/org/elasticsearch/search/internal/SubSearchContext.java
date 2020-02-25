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
package org.elasticsearch.search.internal;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.NamedQuery;
import org.elasticsearch.index.query.NamedSpanQuery;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.collapse.CollapseContext;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchDocValuesContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsContext;
import org.elasticsearch.search.fetch.subphase.highlight.SearchContextHighlight;
import org.elasticsearch.search.fetch.subphase.matches.MatchesContext;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.util.ArrayList;
import java.util.List;

public class SubSearchContext extends FilteredSearchContext {

    // By default return 3 hits per bucket. A higher default would make the response really large by default, since
    // the to hits are returned per bucket.
    private static final int DEFAULT_SIZE = 3;

    private int from;
    private int size = DEFAULT_SIZE;
    private SortAndFormats sort;
    private Query originalQuery;
    private Query query;
    private final List<Query> namedQueries = new ArrayList<>();

    private final FetchSearchResult fetchSearchResult;
    private final QuerySearchResult querySearchResult;

    private int[] docIdsToLoad;
    private int docsIdsToLoadFrom;
    private int docsIdsToLoadSize;

    private StoredFieldsContext storedFields;
    private ScriptFieldsContext scriptFields;
    private FetchSourceContext fetchSourceContext;
    private FetchDocValuesContext docValuesContext;
    private SearchContextHighlight highlight;
    private MatchesContext matches;

    private boolean explain;
    private boolean trackScores;
    private boolean version;
    private boolean seqNoAndPrimaryTerm;

    public SubSearchContext(SearchContext context) {
        super(context);
        this.fetchSearchResult = new FetchSearchResult();
        this.querySearchResult = new QuerySearchResult();
    }

    @Override
    protected void doClose() {
    }

    @Override
    public void preProcess(boolean rewrite) {
    }

    @Override
    public Query buildFilteredQuery(Query query) {
        throw new UnsupportedOperationException("this context should be read only");
    }

    @Override
    public SearchContext scrollContext(ScrollContext scrollContext) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public SearchContext aggregations(SearchContextAggregations aggregations) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public SearchContextHighlight highlight() {
        return highlight;
    }

    @Override
    public void highlight(SearchContextHighlight highlight) {
        this.highlight = highlight;
    }

    @Override
    public MatchesContext matches() {
        return matches;
    }

    @Override
    public void matches(MatchesContext matchesContext) {
        this.matches = matchesContext;
    }

    @Override
    public void suggest(SuggestionSearchContext suggest) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void addRescore(RescoreContext rescore) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public boolean hasScriptFields() {
        return scriptFields != null;
    }

    @Override
    public ScriptFieldsContext scriptFields() {
        if (scriptFields == null) {
            scriptFields = new ScriptFieldsContext();
        }
        return this.scriptFields;
    }

    @Override
    public boolean sourceRequested() {
        return fetchSourceContext != null && fetchSourceContext.fetchSource();
    }

    @Override
    public boolean hasFetchSourceContext() {
        return fetchSourceContext != null;
    }

    @Override
    public FetchSourceContext fetchSourceContext() {
        return fetchSourceContext;
    }

    @Override
    public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
        this.fetchSourceContext = fetchSourceContext;
        return this;
    }

    @Override
    public FetchDocValuesContext docValuesContext() {
        return docValuesContext;
    }

    @Override
    public SearchContext docValuesContext(FetchDocValuesContext docValuesContext) {
        this.docValuesContext = docValuesContext;
        return this;
    }

    @Override
    public void timeout(TimeValue timeout) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void terminateAfter(int terminateAfter) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public SearchContext minimumScore(float minimumScore) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public SearchContext sort(SortAndFormats sort) {
        this.sort = sort;
        return this;
    }

    @Override
    public SortAndFormats sort() {
        return sort;
    }

    @Override
    public Query originalQuery() {
        return originalQuery;
    }

    @Override
    public Query query() {
        return query;
    }

    public void setQuery(Query query) {
        this.originalQuery = query;
        this.query = query;
        this.namedQueries.clear();
        if (this.query != null) {
            this.query.visit(new QueryVisitor() {
                @Override
                public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
                    if (parent instanceof NamedQuery || parent instanceof NamedSpanQuery) {
                        SubSearchContext.this.namedQueries.add(parent);
                    }
                    return super.getSubVisitor(occur, parent);
                }
            });
        }
    }

    @Override
    public List<Query> getNamedQueries() {
        return this.namedQueries;
    }

    @Override
    public SearchContext trackScores(boolean trackScores) {
        this.trackScores = trackScores;
        return this;
    }

    @Override
    public boolean trackScores() {
        return trackScores;
    }

    @Override
    public int from() {
        return from;
    }

    @Override
    public SearchContext from(int from) {
        this.from = from;
        return this;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public SearchContext size(int size) {
        this.size = size;
        return this;
    }

    @Override
    public boolean hasStoredFields() {
        return storedFields != null && storedFields.fieldNames() != null;
    }

    @Override
    public boolean hasStoredFieldsContext() {
        return storedFields != null;
    }

    @Override
    public boolean storedFieldsRequested() {
        return storedFields != null && storedFields.fetchFields();
    }

    @Override
    public StoredFieldsContext storedFieldsContext() {
        return storedFields;
    }

    @Override
    public SearchContext storedFieldsContext(StoredFieldsContext storedFieldsContext) {
        this.storedFields = storedFieldsContext;
        return this;
    }

    @Override
    public boolean explain() {
        return explain;
    }

    @Override
    public void explain(boolean explain) {
        this.explain = explain;
    }

    @Override
    public void groupStats(List<String> groupStats) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public boolean version() {
        return version;
    }

    @Override
    public void version(boolean version) {
        this.version = version;
    }

    @Override
    public boolean seqNoAndPrimaryTerm() {
        return seqNoAndPrimaryTerm;
    }

    @Override
    public void seqNoAndPrimaryTerm(boolean seqNoAndPrimaryTerm) {
        this.seqNoAndPrimaryTerm = seqNoAndPrimaryTerm;
    }

    @Override
    public int[] docIdsToLoad() {
        return docIdsToLoad;
    }

    @Override
    public int docIdsToLoadFrom() {
        return docsIdsToLoadFrom;
    }

    @Override
    public int docIdsToLoadSize() {
        return docsIdsToLoadSize;
    }

    @Override
    public SearchContext docIdsToLoad(int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
        this.docIdsToLoad = docIdsToLoad;
        this.docsIdsToLoadFrom = docsIdsToLoadFrom;
        this.docsIdsToLoadSize = docsIdsToLoadSize;
        return this;
    }

    @Override
    public CollapseContext collapse() {
        return null;
    }

    @Override
    public void accessed(long accessTime) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void keepAlive(long keepAlive) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public QuerySearchResult queryResult() {
        return querySearchResult;
    }

    @Override
    public FetchSearchResult fetchResult() {
        return fetchSearchResult;
    }

    @Override
    public long getRelativeTimeInMillis() {
        throw new UnsupportedOperationException("Not supported");
    }
}
