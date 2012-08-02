/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.search.stats.StatsGroupsParseElement;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.warmer.IndicesWarmer;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.dfs.CachedDfSource;
import org.elasticsearch.search.dfs.DfsPhase;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.*;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.InternalSearchRequest;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.*;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

/**
 *
 */
public class SearchService extends AbstractLifecycleComponent<SearchService> {

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final IndicesWarmer indicesWarmer;

    private final ScriptService scriptService;

    private final DfsPhase dfsPhase;

    private final QueryPhase queryPhase;

    private final FetchPhase fetchPhase;


    private final long defaultKeepAlive;

    private final ScheduledFuture keepAliveReaper;


    private final AtomicLong idGenerator = new AtomicLong();

    private final CleanContextOnIndicesLifecycleListener indicesLifecycleListener = new CleanContextOnIndicesLifecycleListener();

    private final ConcurrentMapLong<SearchContext> activeContexts = ConcurrentCollections.newConcurrentMapLong();

    private final ImmutableMap<String, SearchParseElement> elementParsers;

    @Inject
    public SearchService(Settings settings, ClusterService clusterService, IndicesService indicesService, IndicesLifecycle indicesLifecycle, IndicesWarmer indicesWarmer, ThreadPool threadPool,
                         ScriptService scriptService, DfsPhase dfsPhase, QueryPhase queryPhase, FetchPhase fetchPhase) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.indicesWarmer = indicesWarmer;
        this.scriptService = scriptService;
        this.dfsPhase = dfsPhase;
        this.queryPhase = queryPhase;
        this.fetchPhase = fetchPhase;

        TimeValue keepAliveInterval = componentSettings.getAsTime("keep_alive_interval", timeValueMinutes(1));
        // we can have 5 minutes here, since we make sure to clean with search requests and when shard/index closes
        this.defaultKeepAlive = componentSettings.getAsTime("default_keep_alive", timeValueMinutes(5)).millis();

        Map<String, SearchParseElement> elementParsers = new HashMap<String, SearchParseElement>();
        elementParsers.putAll(dfsPhase.parseElements());
        elementParsers.putAll(queryPhase.parseElements());
        elementParsers.putAll(fetchPhase.parseElements());
        elementParsers.put("stats", new StatsGroupsParseElement());
        this.elementParsers = ImmutableMap.copyOf(elementParsers);
        indicesLifecycle.addListener(indicesLifecycleListener);

        this.keepAliveReaper = threadPool.scheduleWithFixedDelay(new Reaper(), keepAliveInterval);

        this.indicesWarmer.addListener(new SearchWarmer());
    }

    @Override
    protected void doStart() throws ElasticSearchException {
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        for (SearchContext context : activeContexts.values()) {
            freeContext(context);
        }
        activeContexts.clear();
    }

    @Override
    protected void doClose() throws ElasticSearchException {
        keepAliveReaper.cancel(false);
        indicesService.indicesLifecycle().removeListener(indicesLifecycleListener);
    }

    public void releaseContextsForIndex(Index index) {
        for (SearchContext context : activeContexts.values()) {
            if (context.shardTarget().index().equals(index.name())) {
                freeContext(context);
            }
        }
    }

    public void releaseContextsForShard(ShardId shardId) {
        for (SearchContext context : activeContexts.values()) {
            if (context.shardTarget().index().equals(shardId.index().name()) && context.shardTarget().shardId() == shardId.id()) {
                freeContext(context);
            }
        }
    }

    public DfsSearchResult executeDfsPhase(InternalSearchRequest request) throws ElasticSearchException {
        SearchContext context = createContext(request);
        activeContexts.put(context.id(), context);
        try {
            contextProcessing(context);
            dfsPhase.execute(context);
            contextProcessedSuccessfully(context);
            return context.dfsResult();
        } catch (RuntimeException e) {
            logger.trace("Dfs phase failed", e);
            freeContext(context);
            throw e;
        } finally {
            cleanContext(context);
        }
    }

    public QuerySearchResult executeScan(InternalSearchRequest request) throws ElasticSearchException {
        SearchContext context = createContext(request);
        assert context.searchType() == SearchType.SCAN;
        context.searchType(SearchType.COUNT); // move to COUNT, and then, when scrolling, move to SCAN
        activeContexts.put(context.id(), context);
        assert context.searchType() == SearchType.COUNT;
        try {
            if (context.scroll() == null) {
                throw new ElasticSearchException("Scroll must be provided when scanning...");
            }
            contextProcessing(context);
            queryPhase.execute(context);
            contextProcessedSuccessfully(context);
            return context.queryResult();
        } catch (RuntimeException e) {
            logger.trace("Scan phase failed", e);
            freeContext(context);
            throw e;
        } finally {
            cleanContext(context);
        }
    }

    public ScrollQueryFetchSearchResult executeScan(InternalScrollSearchRequest request) throws ElasticSearchException {
        SearchContext context = findContext(request.id());
        contextProcessing(context);
        try {
            processScroll(request, context);
            if (context.searchType() == SearchType.COUNT) {
                // first scanning, reset the from to 0
                context.searchType(SearchType.SCAN);
                context.from(0);
            }
            queryPhase.execute(context);
            shortcutDocIdsToLoadForScanning(context);
            fetchPhase.execute(context);
            if (context.scroll() == null || context.fetchResult().hits().hits().length < context.size()) {
                freeContext(request.id());
            } else {
                contextProcessedSuccessfully(context);
            }
            return new ScrollQueryFetchSearchResult(new QueryFetchSearchResult(context.queryResult(), context.fetchResult()), context.shardTarget());
        } catch (RuntimeException e) {
            logger.trace("Scan phase failed", e);
            freeContext(context);
            throw e;
        } finally {
            cleanContext(context);
        }
    }

    public QuerySearchResult executeQueryPhase(InternalSearchRequest request) throws ElasticSearchException {
        SearchContext context = createContext(request);
        activeContexts.put(context.id(), context);
        try {
            context.indexShard().searchService().onPreQueryPhase(context);
            long time = System.nanoTime();
            contextProcessing(context);
            queryPhase.execute(context);
            if (context.searchType() == SearchType.COUNT) {
                freeContext(context.id());
            } else {
                contextProcessedSuccessfully(context);
            }
            context.indexShard().searchService().onQueryPhase(context, System.nanoTime() - time);
            return context.queryResult();
        } catch (RuntimeException e) {
            context.indexShard().searchService().onFailedQueryPhase(context);
            logger.trace("Query phase failed", e);
            freeContext(context);
            throw e;
        } finally {
            cleanContext(context);
        }
    }

    public ScrollQuerySearchResult executeQueryPhase(InternalScrollSearchRequest request) throws ElasticSearchException {
        SearchContext context = findContext(request.id());
        try {
            context.indexShard().searchService().onPreQueryPhase(context);
            long time = System.nanoTime();
            contextProcessing(context);
            processScroll(request, context);
            queryPhase.execute(context);
            contextProcessedSuccessfully(context);
            context.indexShard().searchService().onQueryPhase(context, System.nanoTime() - time);
            return new ScrollQuerySearchResult(context.queryResult(), context.shardTarget());
        } catch (RuntimeException e) {
            context.indexShard().searchService().onFailedQueryPhase(context);
            logger.trace("Query phase failed", e);
            freeContext(context);
            throw e;
        } finally {
            cleanContext(context);
        }
    }

    public QuerySearchResult executeQueryPhase(QuerySearchRequest request) throws ElasticSearchException {
        SearchContext context = findContext(request.id());
        contextProcessing(context);
        try {
            context.searcher().dfSource(new CachedDfSource(request.dfs(), context.similarityService().defaultSearchSimilarity()));
        } catch (IOException e) {
            freeContext(context);
            cleanContext(context);
            throw new QueryPhaseExecutionException(context, "Failed to set aggregated df", e);
        }
        try {
            context.indexShard().searchService().onPreQueryPhase(context);
            long time = System.nanoTime();
            queryPhase.execute(context);
            contextProcessedSuccessfully(context);
            context.indexShard().searchService().onQueryPhase(context, System.nanoTime() - time);
            return context.queryResult();
        } catch (RuntimeException e) {
            context.indexShard().searchService().onFailedQueryPhase(context);
            logger.trace("Query phase failed", e);
            freeContext(context);
            throw e;
        } finally {
            cleanContext(context);
        }
    }

    public QueryFetchSearchResult executeFetchPhase(InternalSearchRequest request) throws ElasticSearchException {
        SearchContext context = createContext(request);
        activeContexts.put(context.id(), context);
        contextProcessing(context);
        try {
            context.indexShard().searchService().onPreQueryPhase(context);
            long time = System.nanoTime();
            try {
                queryPhase.execute(context);
            } catch (RuntimeException e) {
                context.indexShard().searchService().onFailedQueryPhase(context);
                throw e;
            }
            long time2 = System.nanoTime();
            context.indexShard().searchService().onQueryPhase(context, time2 - time);
            context.indexShard().searchService().onPreFetchPhase(context);
            try {
                shortcutDocIdsToLoad(context);
                fetchPhase.execute(context);
                if (context.scroll() == null) {
                    freeContext(context.id());
                } else {
                    contextProcessedSuccessfully(context);
                }
            } catch (RuntimeException e) {
                context.indexShard().searchService().onFailedFetchPhase(context);
                throw e;
            }
            context.indexShard().searchService().onFetchPhase(context, System.nanoTime() - time2);
            return new QueryFetchSearchResult(context.queryResult(), context.fetchResult());
        } catch (RuntimeException e) {
            logger.trace("Fetch phase failed", e);
            freeContext(context);
            throw e;
        } finally {
            cleanContext(context);
        }
    }

    public QueryFetchSearchResult executeFetchPhase(QuerySearchRequest request) throws ElasticSearchException {
        SearchContext context = findContext(request.id());
        contextProcessing(context);
        try {
            context.searcher().dfSource(new CachedDfSource(request.dfs(), context.similarityService().defaultSearchSimilarity()));
        } catch (IOException e) {
            freeContext(context);
            cleanContext(context);
            throw new QueryPhaseExecutionException(context, "Failed to set aggregated df", e);
        }
        try {
            context.indexShard().searchService().onPreQueryPhase(context);
            long time = System.nanoTime();
            try {
                queryPhase.execute(context);
            } catch (RuntimeException e) {
                context.indexShard().searchService().onFailedQueryPhase(context);
                throw e;
            }
            long time2 = System.nanoTime();
            context.indexShard().searchService().onQueryPhase(context, time2 - time);
            context.indexShard().searchService().onPreFetchPhase(context);
            try {
                shortcutDocIdsToLoad(context);
                fetchPhase.execute(context);
                if (context.scroll() == null) {
                    freeContext(request.id());
                } else {
                    contextProcessedSuccessfully(context);
                }
            } catch (RuntimeException e) {
                context.indexShard().searchService().onFailedFetchPhase(context);
                throw e;
            }
            context.indexShard().searchService().onFetchPhase(context, System.nanoTime() - time2);
            return new QueryFetchSearchResult(context.queryResult(), context.fetchResult());
        } catch (RuntimeException e) {
            logger.trace("Fetch phase failed", e);
            freeContext(context);
            throw e;
        } finally {
            cleanContext(context);
        }
    }

    public ScrollQueryFetchSearchResult executeFetchPhase(InternalScrollSearchRequest request) throws ElasticSearchException {
        SearchContext context = findContext(request.id());
        contextProcessing(context);
        try {
            processScroll(request, context);
            context.indexShard().searchService().onPreQueryPhase(context);
            long time = System.nanoTime();
            try {
                queryPhase.execute(context);
            } catch (RuntimeException e) {
                context.indexShard().searchService().onFailedQueryPhase(context);
                throw e;
            }
            long time2 = System.nanoTime();
            context.indexShard().searchService().onQueryPhase(context, time2 - time);
            context.indexShard().searchService().onPreFetchPhase(context);
            try {
                shortcutDocIdsToLoad(context);
                fetchPhase.execute(context);
                if (context.scroll() == null) {
                    freeContext(request.id());
                } else {
                    contextProcessedSuccessfully(context);
                }
            } catch (RuntimeException e) {
                context.indexShard().searchService().onFailedFetchPhase(context);
                throw e;
            }
            context.indexShard().searchService().onFetchPhase(context, System.nanoTime() - time2);
            return new ScrollQueryFetchSearchResult(new QueryFetchSearchResult(context.queryResult(), context.fetchResult()), context.shardTarget());
        } catch (RuntimeException e) {
            logger.trace("Fetch phase failed", e);
            freeContext(context);
            throw e;
        } finally {
            cleanContext(context);
        }
    }

    public FetchSearchResult executeFetchPhase(FetchSearchRequest request) throws ElasticSearchException {
        SearchContext context = findContext(request.id());
        contextProcessing(context);
        try {
            context.docIdsToLoad(request.docIds(), 0, request.docIdsSize());
            context.indexShard().searchService().onPreFetchPhase(context);
            long time = System.nanoTime();
            fetchPhase.execute(context);
            if (context.scroll() == null) {
                freeContext(request.id());
            } else {
                contextProcessedSuccessfully(context);
            }
            context.indexShard().searchService().onFetchPhase(context, System.nanoTime() - time);
            return context.fetchResult();
        } catch (RuntimeException e) {
            context.indexShard().searchService().onFailedFetchPhase(context);
            logger.trace("Fetch phase failed", e);
            freeContext(context);
            throw e;
        } finally {
            cleanContext(context);
        }
    }

    private SearchContext findContext(long id) throws SearchContextMissingException {
        SearchContext context = activeContexts.get(id);
        if (context == null) {
            throw new SearchContextMissingException(id);
        }
        SearchContext.setCurrent(context);
        return context;
    }

    SearchContext createContext(InternalSearchRequest request) throws ElasticSearchException {
        return createContext(request, null);
    }

    SearchContext createContext(InternalSearchRequest request, @Nullable Engine.Searcher searcher) throws ElasticSearchException {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(request.shardId());

        SearchShardTarget shardTarget = new SearchShardTarget(clusterService.localNode().id(), request.index(), request.shardId());

        Engine.Searcher engineSearcher = searcher == null ? indexShard.searcher() : searcher;
        SearchContext context = new SearchContext(idGenerator.incrementAndGet(), request, shardTarget, engineSearcher, indexService, indexShard, scriptService);
        SearchContext.setCurrent(context);
        try {
            context.scroll(request.scroll());

            parseSource(context, request.source());
            parseSource(context, request.extraSource());

            // if the from and size are still not set, default them
            if (context.from() == -1) {
                context.from(0);
            }
            if (context.size() == -1) {
                context.size(10);
            }

            Filter aliasFilter = indexService.aliasesService().aliasFilter(request.filteringAliases());
            context.aliasFilter(aliasFilter);

            // pre process
            dfsPhase.preProcess(context);
            queryPhase.preProcess(context);
            fetchPhase.preProcess(context);

            // compute the context keep alive
            long keepAlive = defaultKeepAlive;
            if (request.scroll() != null && request.scroll().keepAlive() != null) {
                keepAlive = request.scroll().keepAlive().millis();
            }
            context.keepAlive(keepAlive);
        } catch (RuntimeException e) {
            context.release();
            throw e;
        }

        return context;
    }

    public void freeContext(long id) {
        SearchContext context = activeContexts.remove(id);
        if (context == null) {
            return;
        }
        freeContext(context);
    }

    private void freeContext(SearchContext context) {
        activeContexts.remove(context.id());
        context.release();
    }

    private void contextProcessing(SearchContext context) {
        // disable timeout while executing a search
        context.accessed(-1);
    }

    private void contextProcessedSuccessfully(SearchContext context) {
        context.accessed(threadPool.estimatedTimeInMillis());
    }

    private void cleanContext(SearchContext context) {
        SearchContext.removeCurrent();
    }

    private void parseSource(SearchContext context, BytesReference source) throws SearchParseException {
        // nothing to parse...
        if (source == null || source.length() == 0) {
            return;
        }
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    parser.nextToken();
                    SearchParseElement element = elementParsers.get(fieldName);
                    if (element == null) {
                        throw new SearchParseException(context, "No parser for element [" + fieldName + "]");
                    }
                    element.parse(parser, context);
                } else if (token == null) {
                    break;
                }
            }
        } catch (Exception e) {
            String sSource = "_na_";
            try {
                sSource = XContentHelper.convertToJson(source, false);
            } catch (Throwable e1) {
                // ignore
            }
            throw new SearchParseException(context, "Failed to parse source [" + sSource + "]", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    private static final int[] EMPTY_DOC_IDS = new int[0];

    /**
     * Shortcut ids to load, we load only "from" and up to "size". The phase controller
     * handles this as well since the result is always size * shards for Q_A_F
     */
    private void shortcutDocIdsToLoad(SearchContext context) {
        TopDocs topDocs = context.queryResult().topDocs();
        if (topDocs.scoreDocs.length < context.from()) {
            // no more docs...
            context.docIdsToLoad(EMPTY_DOC_IDS, 0, 0);
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
        context.docIdsToLoad(docIdsToLoad, 0, counter);
    }

    private void shortcutDocIdsToLoadForScanning(SearchContext context) {
        TopDocs topDocs = context.queryResult().topDocs();
        if (topDocs.scoreDocs.length == 0) {
            // no more docs...
            context.docIdsToLoad(EMPTY_DOC_IDS, 0, 0);
            return;
        }
        int[] docIdsToLoad = new int[topDocs.scoreDocs.length];
        for (int i = 0; i < docIdsToLoad.length; i++) {
            docIdsToLoad[i] = topDocs.scoreDocs[i].doc;
        }
        context.docIdsToLoad(docIdsToLoad, 0, docIdsToLoad.length);
    }

    private void processScroll(InternalScrollSearchRequest request, SearchContext context) {
        // process scroll
        context.from(context.from() + context.size());
        context.scroll(request.scroll());
        // update the context keep alive based on the new scroll value
        if (request.scroll() != null && request.scroll().keepAlive() != null) {
            context.keepAlive(request.scroll().keepAlive().millis());
        }
    }

    class SearchWarmer implements IndicesWarmer.Listener {

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }

        @Override
        public void warm(IndexShard indexShard, IndexMetaData indexMetaData, IndicesWarmer.WarmerContext warmerContext) {
            IndexWarmersMetaData custom = indexMetaData.custom(IndexWarmersMetaData.TYPE);
            if (custom == null) {
                return;
            }
            for (IndexWarmersMetaData.Entry entry : custom.entries()) {
                SearchContext context = null;
                try {
                    long now = System.nanoTime();
                    InternalSearchRequest request = new InternalSearchRequest(indexShard.shardId().index().name(), indexShard.shardId().id(), indexMetaData.numberOfShards(), SearchType.COUNT)
                            .source(entry.source())
                            .types(entry.types());
                    context = createContext(request, warmerContext.newSearcher());
                    queryPhase.execute(context);
                    long took = System.nanoTime() - now;
                    if (indexShard.warmerService().logger().isTraceEnabled()) {
                        indexShard.warmerService().logger().trace("warmed [{}], took [{}]", entry.name(), TimeValue.timeValueNanos(took));
                    }
                } catch (Throwable t) {
                    indexShard.warmerService().logger().warn("warmer [{}] failed", t, entry.name());
                } finally {
                    if (context != null) {
                        freeContext(context);
                        cleanContext(context);
                    }
                }
            }
        }
    }

    class CleanContextOnIndicesLifecycleListener extends IndicesLifecycle.Listener {

        @Override
        public void beforeIndexClosed(IndexService indexService, boolean delete) {
            releaseContextsForIndex(indexService.index());
        }

        @Override
        public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, boolean delete) {
            releaseContextsForShard(shardId);
        }
    }

    class Reaper implements Runnable {
        @Override
        public void run() {
            long time = threadPool.estimatedTimeInMillis();
            for (SearchContext context : activeContexts.values()) {
                if (context.lastAccessTime() == -1) { // its being processed or timeout is disabled
                    continue;
                }
                if ((time - context.lastAccessTime() > context.keepAlive())) {
                    freeContext(context);
                }
            }
        }
    }
}
