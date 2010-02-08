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

package org.elasticsearch.search;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.lucene.search.TopDocs;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.dfs.CachedDfSource;
import org.elasticsearch.search.dfs.DfsPhase;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchRequest;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.InternalSearchRequest;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.component.Lifecycle;
import org.elasticsearch.util.component.LifecycleComponent;
import org.elasticsearch.util.concurrent.highscalelib.NonBlockingHashMapLong;
import org.elasticsearch.util.io.FastStringReader;
import org.elasticsearch.util.json.Jackson;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author kimchy (Shay Banon)
 */
public class SearchService extends AbstractComponent implements LifecycleComponent<SearchService> {

    private final Lifecycle lifecycle = new Lifecycle();

    private final JsonFactory jsonFactory = Jackson.defaultJsonFactory();

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final DfsPhase dfsPhase;

    private final QueryPhase queryPhase;

    private final FetchPhase fetchPhase;

    private final AtomicLong idGenerator = new AtomicLong();

    private final NonBlockingHashMapLong<SearchContext> activeContexts = new NonBlockingHashMapLong<SearchContext>();

    private final ImmutableMap<String, SearchParseElement> elementParsers;

    @Inject public SearchService(Settings settings, ClusterService clusterService, IndicesService indicesService,
                                 DfsPhase dfsPhase, QueryPhase queryPhase, FetchPhase fetchPhase) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.dfsPhase = dfsPhase;
        this.queryPhase = queryPhase;
        this.fetchPhase = fetchPhase;

        Map<String, SearchParseElement> elementParsers = new HashMap<String, SearchParseElement>();
        elementParsers.putAll(dfsPhase.parseElements());
        elementParsers.putAll(queryPhase.parseElements());
        elementParsers.putAll(fetchPhase.parseElements());
        this.elementParsers = ImmutableMap.copyOf(elementParsers);
    }

    @Override public Lifecycle.State lifecycleState() {
        return lifecycle.state();
    }

    @Override public SearchService start() throws ElasticSearchException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        return this;
    }

    @Override public SearchService stop() throws ElasticSearchException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        for (SearchContext context : activeContexts.values()) {
            freeContext(context);
        }
        activeContexts.clear();
        return this;
    }

    @Override public void close() throws ElasticSearchException {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
    }

    public DfsSearchResult executeDfsPhase(InternalSearchRequest request) throws ElasticSearchException {
        SearchContext context = createContext(request);
        activeContexts.put(context.id(), context);
        dfsPhase.execute(context);
        return context.dfsResult();
    }

    public QuerySearchResult executeQueryPhase(InternalSearchRequest request) throws ElasticSearchException {
        SearchContext context = createContext(request);
        activeContexts.put(context.id(), context);
        queryPhase.execute(context);
        return context.queryResult();
    }

    public QuerySearchResult executeQueryPhase(InternalScrollSearchRequest request) throws ElasticSearchException {
        SearchContext context = findContext(request.id());
        processScroll(request, context);
        queryPhase.execute(context);
        return context.queryResult();
    }

    public QuerySearchResult executeQueryPhase(QuerySearchRequest request) throws ElasticSearchException {
        SearchContext context = findContext(request.id());
        try {
            context.searcher().dfSource(new CachedDfSource(request.dfs(), context.similarityService().defaultSearchSimilarity()));
        } catch (IOException e) {
            throw new SearchException("Failed to set aggreagted df", e);
        }
        queryPhase.execute(context);
        return context.queryResult();
    }

    public QueryFetchSearchResult executeFetchPhase(InternalSearchRequest request) throws ElasticSearchException {
        SearchContext context = createContext(request);
        queryPhase.execute(context);
        shortcutDocIdsToLoad(context);
        fetchPhase.execute(context);
        if (context.scroll() != null) {
            activeContexts.put(context.id(), context);
        }
        return new QueryFetchSearchResult(context.queryResult(), context.fetchResult());
    }

    public QueryFetchSearchResult executeFetchPhase(QuerySearchRequest request) throws ElasticSearchException {
        SearchContext context = findContext(request.id());
        try {
            context.searcher().dfSource(new CachedDfSource(request.dfs(), context.similarityService().defaultSearchSimilarity()));
        } catch (IOException e) {
            throw new SearchException("Failed to set aggregated df", e);
        }
        queryPhase.execute(context);
        shortcutDocIdsToLoad(context);
        fetchPhase.execute(context);
        if (context.scroll() != null) {
            activeContexts.put(context.id(), context);
        }
        return new QueryFetchSearchResult(context.queryResult(), context.fetchResult());
    }

    public QueryFetchSearchResult executeFetchPhase(InternalScrollSearchRequest request) throws ElasticSearchException {
        SearchContext context = findContext(request.id());
        processScroll(request, context);
        queryPhase.execute(context);
        shortcutDocIdsToLoad(context);
        fetchPhase.execute(context);
        if (context.scroll() == null) {
            freeContext(request.id());
        }
        return new QueryFetchSearchResult(context.queryResult(), context.fetchResult());
    }

    public FetchSearchResult executeFetchPhase(FetchSearchRequest request) throws ElasticSearchException {
        SearchContext context = findContext(request.id());
        context.docIdsToLoad(request.docIds());
        fetchPhase.execute(context);
        if (context.scroll() == null) {
            freeContext(request.id());
        }
        return context.fetchResult();
    }

    private SearchContext findContext(long id) throws SearchContextMissingException {
        SearchContext context = activeContexts.get(id);
        if (context == null) {
            throw new SearchContextMissingException(id);
        }
        return context;
    }

    private SearchContext createContext(InternalSearchRequest request) throws ElasticSearchException {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(request.shardId());
        Engine.Searcher engineSearcher = indexShard.searcher();

        SearchShardTarget shardTarget = new SearchShardTarget(clusterService.state().nodes().localNodeId(), request.index(), request.shardId());

        SearchContext context = new SearchContext(idGenerator.incrementAndGet(), shardTarget, request.timeout(),
                request.queryBoost(), request.source(), request.types(), engineSearcher, indexService);

        // init the from and size
        context.from(request.from());
        context.size(request.size());

        context.scroll(request.scroll());

        parseSource(context);

        // if the from and size are still not set, default them
        if (context.from() == -1) {
            context.from(0);
        }
        if (context.size() == -1) {
            context.size(10);
        }

        return context;
    }

    private void freeContext(long id) {
        SearchContext context = activeContexts.remove(id);
        if (context == null) {
            return;
        }
        freeContext(context);
    }

    private void freeContext(SearchContext context) {
        context.release();
    }

    private void parseSource(SearchContext context) throws SearchParseException {
        try {
            JsonParser jp = jsonFactory.createJsonParser(new FastStringReader(context.source()));
            JsonToken token;
            while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
                if (token == JsonToken.FIELD_NAME) {
                    String fieldName = jp.getCurrentName();
                    jp.nextToken();
                    SearchParseElement element = elementParsers.get(fieldName);
                    if (element == null) {
                        throw new SearchParseException("No parser for element [" + fieldName + "]");
                    }
                    element.parse(jp, context);
                } else if (token == null) {
                    break;
                }
            }
        } catch (Exception e) {
            throw new SearchParseException("Failed to parse [" + context.source() + "]", e);
        }
    }

    private void shortcutDocIdsToLoad(SearchContext context) {
        TopDocs topDocs = context.queryResult().topDocs();
        if (topDocs.scoreDocs.length < context.from()) {
            // no more docs...
            context.docIdsToLoad(new int[0]);
            return;
        }
        int totalSize = context.from() + context.size();
        int[] docIdsToLoad = new int[context.size()];
        int counter = 0;
        for (int i = context.from(); i < totalSize; i++) {
            if (i < topDocs.scoreDocs.length) {
                docIdsToLoad[counter] = topDocs.scoreDocs[i].doc;
            } else {
                break;
            }
            counter++;
        }
        if (counter < context.size()) {
            docIdsToLoad = Arrays.copyOfRange(docIdsToLoad, 0, counter);
        }
        context.docIdsToLoad(docIdsToLoad);
    }

    private void processScroll(InternalScrollSearchRequest request, SearchContext context) {
        // process scroll
        context.from(context.from() + context.size());
        context.scroll(request.scroll());
    }
}
