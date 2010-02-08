/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.IndexQueryParser;
import org.elasticsearch.index.query.IndexQueryParserMissingException;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.lease.Releasable;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class SearchContext implements Releasable {

    private final long id;

    private final String source;

    private final Engine.Searcher engineSearcher;

    private final IndexService indexService;

    private final ContextIndexSearcher searcher;

    private final DfsSearchResult dfsResult;

    private final QuerySearchResult queryResult;

    private final FetchSearchResult fetchResult;

    private final TimeValue timeout;

    private final float queryBoost;


    private Scroll scroll;

    private boolean explain;

    private String[] fieldNames;

    private int from = -1;

    private int size = -1;

    private String[] types;

    private Sort sort;

    private String queryParserName;

    private Query query;

    private int[] docIdsToLoad;

    private SearchContextFacets facets;


    private boolean queryRewritten;

    public SearchContext(long id, SearchShardTarget shardTarget, TimeValue timeout, float queryBoost, String source,
                         String[] types, Engine.Searcher engineSearcher, IndexService indexService) {
        this.id = id;
        this.timeout = timeout;
        this.queryBoost = queryBoost;
        this.source = source;
        this.types = types;
        this.engineSearcher = engineSearcher;
        this.dfsResult = new DfsSearchResult(id, shardTarget);
        this.queryResult = new QuerySearchResult(id, shardTarget);
        this.fetchResult = new FetchSearchResult(id, shardTarget);
        this.indexService = indexService;

        this.searcher = new ContextIndexSearcher(this, engineSearcher.reader());
    }

    @Override public boolean release() throws ElasticSearchException {
        try {
            searcher.close();
        } catch (IOException e) {
            // ignore this exception
        }
        engineSearcher.release();
        return true;
    }

    public long id() {
        return this.id;
    }

    public String source() {
        return source;
    }

    public String[] types() {
        return types;
    }

    public float queryBoost() {
        return queryBoost;
    }

    public Scroll scroll() {
        return this.scroll;
    }

    public SearchContext scroll(Scroll scroll) {
        this.scroll = scroll;
        return this;
    }

    public SearchContextFacets facets() {
        return facets;
    }

    public SearchContext facets(SearchContextFacets facets) {
        this.facets = facets;
        return this;
    }

    public Engine.Searcher engineSearcher() {
        return this.engineSearcher;
    }

    public ContextIndexSearcher searcher() {
        return this.searcher;
    }

    public IndexQueryParser queryParser() throws IndexQueryParserMissingException {
        if (queryParserName != null) {
            IndexQueryParser queryParser = queryParserService().indexQueryParser(queryParserName);
            if (queryParser == null) {
                throw new IndexQueryParserMissingException(queryParserName);
            }
            return queryParser;
        }
        return queryParserService().defaultIndexQueryParser();
    }

    public MapperService mapperService() {
        return indexService.mapperService();
    }

    public IndexQueryParserService queryParserService() {
        return indexService.queryParserService();
    }

    public SimilarityService similarityService() {
        return indexService.similarityService();
    }

    public FilterCache filterCache() {
        return indexService.filterCache();
    }

    public TimeValue timeout() {
        return timeout;
    }

    public SearchContext sort(Sort sort) {
        this.sort = sort;
        return this;
    }

    public Sort sort() {
        return this.sort;
    }

    public String queryParserName() {
        return queryParserName;
    }

    public SearchContext queryParserName(String queryParserName) {
        this.queryParserName = queryParserName;
        return this;
    }

    public SearchContext query(Query query) {
        if (query == null) {
            this.query = query;
            return this;
        }
        queryRewritten = false;
        this.query = query;
        return this;
    }

    public Query query() {
        return this.query;
    }

    public int from() {
        return from;
    }

    public SearchContext from(int from) {
        this.from = from;
        return this;
    }

    public int size() {
        return size;
    }

    public SearchContext size(int size) {
        this.size = size;
        return this;
    }

    public String[] fieldNames() {
        return fieldNames;
    }

    public SearchContext fieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
        return this;
    }

    public boolean explain() {
        return explain;
    }

    public void explain(boolean explain) {
        this.explain = explain;
    }

    public SearchContext rewriteQuery() throws IOException {
        if (queryRewritten) {
            return this;
        }
        query = query.rewrite(searcher.getIndexReader());
        queryRewritten = true;
        return this;
    }

    public int[] docIdsToLoad() {
        return docIdsToLoad;
    }

    public SearchContext docIdsToLoad(int[] docIdsToLoad) {
        this.docIdsToLoad = docIdsToLoad;
        return this;
    }

    public DfsSearchResult dfsResult() {
        return dfsResult;
    }

    public QuerySearchResult queryResult() {
        return queryResult;
    }

    public FetchSearchResult fetchResult() {
        return fetchResult;
    }
}
