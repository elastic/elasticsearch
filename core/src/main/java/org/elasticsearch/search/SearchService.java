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

package org.elasticsearch.search;

import com.carrotsearch.hppc.ObjectFloatHashMap;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.search.stats.ShardSearchStats;
import org.elasticsearch.index.search.stats.StatsGroupsParseElement;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.cache.request.IndicesRequestCache;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.dfs.DfsPhase;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ScrollQueryFetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchRequest;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsContext;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsContext.FieldDataField;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.script.ScriptFieldsContext.ScriptField;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SearchContext.Lifetime;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.search.query.ScrollQuerySearchResult;
import org.elasticsearch.search.rescore.RescoreBuilder;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

/**
 *
 */
public class SearchService extends AbstractLifecycleComponent<SearchService> implements IndexEventListener {

    // we can have 5 minutes here, since we make sure to clean with search requests and when shard/index closes
    public static final Setting<TimeValue> DEFAULT_KEEPALIVE_SETTING = Setting.positiveTimeSetting("search.default_keep_alive", timeValueMinutes(5), false, Setting.Scope.CLUSTER);
    public static final Setting<TimeValue> KEEPALIVE_INTERVAL_SETTING = Setting.positiveTimeSetting("search.keep_alive_interval", timeValueMinutes(1), false, Setting.Scope.CLUSTER);

    public static final TimeValue NO_TIMEOUT = timeValueMillis(-1);
    public static final Setting<TimeValue> DEFAULT_SEARCH_TIMEOUT_SETTING = Setting.timeSetting("search.default_search_timeout", NO_TIMEOUT, true, Setting.Scope.CLUSTER);


    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final ScriptService scriptService;

    private final PageCacheRecycler pageCacheRecycler;

    private final BigArrays bigArrays;

    private final DfsPhase dfsPhase;

    private final QueryPhase queryPhase;

    private final FetchPhase fetchPhase;

    private final IndicesRequestCache indicesQueryCache;

    private final long defaultKeepAlive;

    private volatile TimeValue defaultSearchTimeout;

    private final ScheduledFuture<?> keepAliveReaper;

    private final AtomicLong idGenerator = new AtomicLong();

    private final ConcurrentMapLong<SearchContext> activeContexts = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();

    private final Map<String, SearchParseElement> elementParsers;

    private final ParseFieldMatcher parseFieldMatcher;

    @Inject
    public SearchService(Settings settings, ClusterSettings clusterSettings, ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool,
                         ScriptService scriptService, PageCacheRecycler pageCacheRecycler, BigArrays bigArrays, DfsPhase dfsPhase, QueryPhase queryPhase, FetchPhase fetchPhase,
                         IndicesRequestCache indicesQueryCache) {
        super(settings);
        this.parseFieldMatcher = new ParseFieldMatcher(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.scriptService = scriptService;
        this.pageCacheRecycler = pageCacheRecycler;
        this.bigArrays = bigArrays;
        this.dfsPhase = dfsPhase;
        this.queryPhase = queryPhase;
        this.fetchPhase = fetchPhase;
        this.indicesQueryCache = indicesQueryCache;

        TimeValue keepAliveInterval = KEEPALIVE_INTERVAL_SETTING.get(settings);
        this.defaultKeepAlive = DEFAULT_KEEPALIVE_SETTING.get(settings).millis();

        Map<String, SearchParseElement> elementParsers = new HashMap<>();
        elementParsers.putAll(dfsPhase.parseElements());
        elementParsers.putAll(queryPhase.parseElements());
        elementParsers.putAll(fetchPhase.parseElements());
        elementParsers.put("stats", new StatsGroupsParseElement());
        this.elementParsers = unmodifiableMap(elementParsers);

        this.keepAliveReaper = threadPool.scheduleWithFixedDelay(new Reaper(), keepAliveInterval);

        defaultSearchTimeout = DEFAULT_SEARCH_TIMEOUT_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(DEFAULT_SEARCH_TIMEOUT_SETTING, this::setDefaultSearchTimeout);
    }

    private void setDefaultSearchTimeout(TimeValue defaultSearchTimeout) {
        this.defaultSearchTimeout = defaultSearchTimeout;
    }

    @Override
    public void afterIndexClosed(Index index, Settings indexSettings) {
        // once an index is closed we can just clean up all the pending search context information
        // to release memory and let references to the filesystem go etc.
        IndexMetaData idxMeta = SearchService.this.clusterService.state().metaData().index(index.getName());
        if (idxMeta != null && idxMeta.getState() == IndexMetaData.State.CLOSE) {
            // we need to check if it's really closed
            // since sometimes due to a relocation we already closed the shard and that causes the index to be closed
            // if we then close all the contexts we can get some search failures along the way which are not expected.
            // it's fine to keep the contexts open if the index is still "alive"
            // unfortunately we don't have a clear way to signal today why an index is closed.
            afterIndexDeleted(index, indexSettings);
        }
    }

    @Override
    public void afterIndexDeleted(Index index, Settings indexSettings) {
        freeAllContextForIndex(index);
    }

    protected void putContext(SearchContext context) {
        final SearchContext previous = activeContexts.put(context.id(), context);
        assert previous == null;
    }

    protected SearchContext removeContext(long id) {
        return activeContexts.remove(id);
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
        for (final SearchContext context : activeContexts.values()) {
            freeContext(context.id());
        }
    }

    @Override
    protected void doClose() {
        doStop();
        FutureUtils.cancel(keepAliveReaper);
    }

    public DfsSearchResult executeDfsPhase(ShardSearchRequest request) {
        final SearchContext context = createAndPutContext(request);
        try {
            contextProcessing(context);
            dfsPhase.execute(context);
            contextProcessedSuccessfully(context);
            return context.dfsResult();
        } catch (Throwable e) {
            logger.trace("Dfs phase failed", e);
            processFailure(context, e);
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            cleanContext(context);
        }
    }

    /**
     * Try to load the query results from the cache or execute the query phase directly if the cache cannot be used.
     */
    private void loadOrExecuteQueryPhase(final ShardSearchRequest request, final SearchContext context,
            final QueryPhase queryPhase) throws Exception {
        final boolean canCache = indicesQueryCache.canCache(request, context);
        if (canCache) {
            indicesQueryCache.loadIntoContext(request, context, queryPhase);
        } else {
            queryPhase.execute(context);
        }
    }

    public QuerySearchResultProvider executeQueryPhase(ShardSearchRequest request) {
        final SearchContext context = createAndPutContext(request);
        final ShardSearchStats shardSearchStats = context.indexShard().searchService();
        try {
            shardSearchStats.onPreQueryPhase(context);
            long time = System.nanoTime();
            contextProcessing(context);

            loadOrExecuteQueryPhase(request, context, queryPhase);

            if (context.queryResult().topDocs().scoreDocs.length == 0 && context.scrollContext() == null) {
                freeContext(context.id());
            } else {
                contextProcessedSuccessfully(context);
            }
            shardSearchStats.onQueryPhase(context, System.nanoTime() - time);

            return context.queryResult();
        } catch (Throwable e) {
            // execution exception can happen while loading the cache, strip it
            if (e instanceof ExecutionException) {
                e = e.getCause();
            }
            shardSearchStats.onFailedQueryPhase(context);
            logger.trace("Query phase failed", e);
            processFailure(context, e);
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            cleanContext(context);
        }
    }

    public ScrollQuerySearchResult executeQueryPhase(InternalScrollSearchRequest request) {
        final SearchContext context = findContext(request.id());
        ShardSearchStats shardSearchStats = context.indexShard().searchService();
        try {
            shardSearchStats.onPreQueryPhase(context);
            long time = System.nanoTime();
            contextProcessing(context);
            processScroll(request, context);
            queryPhase.execute(context);
            contextProcessedSuccessfully(context);
            shardSearchStats.onQueryPhase(context, System.nanoTime() - time);
            return new ScrollQuerySearchResult(context.queryResult(), context.shardTarget());
        } catch (Throwable e) {
            shardSearchStats.onFailedQueryPhase(context);
            logger.trace("Query phase failed", e);
            processFailure(context, e);
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            cleanContext(context);
        }
    }

    public QuerySearchResult executeQueryPhase(QuerySearchRequest request) {
        final SearchContext context = findContext(request.id());
        contextProcessing(context);
        context.searcher().setAggregatedDfs(request.dfs());
        IndexShard indexShard = context.indexShard();
        ShardSearchStats shardSearchStats = indexShard.searchService();
        try {
            shardSearchStats.onPreQueryPhase(context);
            long time = System.nanoTime();
            queryPhase.execute(context);
            if (context.queryResult().topDocs().scoreDocs.length == 0 && context.scrollContext() == null) {
                // no hits, we can release the context since there will be no fetch phase
                freeContext(context.id());
            } else {
                contextProcessedSuccessfully(context);
            }
            shardSearchStats.onQueryPhase(context, System.nanoTime() - time);
            return context.queryResult();
        } catch (Throwable e) {
            shardSearchStats.onFailedQueryPhase(context);
            logger.trace("Query phase failed", e);
            processFailure(context, e);
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            cleanContext(context);
        }
    }

    private boolean fetchPhaseShouldFreeContext(SearchContext context) {
        if (context.scrollContext() == null) {
            // simple search, no scroll
            return true;
        } else {
            // scroll request, but the scroll was not extended
            return context.scrollContext().scroll == null;
        }
    }

    public QueryFetchSearchResult executeFetchPhase(ShardSearchRequest request) {
        final SearchContext context = createAndPutContext(request);
        contextProcessing(context);
        try {
            ShardSearchStats shardSearchStats = context.indexShard().searchService();
            shardSearchStats.onPreQueryPhase(context);
            long time = System.nanoTime();
            try {
                loadOrExecuteQueryPhase(request, context, queryPhase);
            } catch (Throwable e) {
                shardSearchStats.onFailedQueryPhase(context);
                throw ExceptionsHelper.convertToRuntime(e);
            }
            long time2 = System.nanoTime();
            shardSearchStats.onQueryPhase(context, time2 - time);
            shardSearchStats.onPreFetchPhase(context);
            try {
                shortcutDocIdsToLoad(context);
                fetchPhase.execute(context);
                if (fetchPhaseShouldFreeContext(context)) {
                    freeContext(context.id());
                } else {
                    contextProcessedSuccessfully(context);
                }
            } catch (Throwable e) {
                shardSearchStats.onFailedFetchPhase(context);
                throw ExceptionsHelper.convertToRuntime(e);
            }
            shardSearchStats.onFetchPhase(context, System.nanoTime() - time2);
            return new QueryFetchSearchResult(context.queryResult(), context.fetchResult());
        } catch (Throwable e) {
            logger.trace("Fetch phase failed", e);
            processFailure(context, e);
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            cleanContext(context);
        }
    }

    public QueryFetchSearchResult executeFetchPhase(QuerySearchRequest request) {
        final SearchContext context = findContext(request.id());
        contextProcessing(context);
        context.searcher().setAggregatedDfs(request.dfs());
        try {
            ShardSearchStats shardSearchStats = context.indexShard().searchService();
            shardSearchStats.onPreQueryPhase(context);
            long time = System.nanoTime();
            try {
                queryPhase.execute(context);
            } catch (Throwable e) {
                shardSearchStats.onFailedQueryPhase(context);
                throw ExceptionsHelper.convertToRuntime(e);
            }
            long time2 = System.nanoTime();
            shardSearchStats.onQueryPhase(context, time2 - time);
            shardSearchStats.onPreFetchPhase(context);
            try {
                shortcutDocIdsToLoad(context);
                fetchPhase.execute(context);
                if (fetchPhaseShouldFreeContext(context)) {
                    freeContext(request.id());
                } else {
                    contextProcessedSuccessfully(context);
                }
            } catch (Throwable e) {
                shardSearchStats.onFailedFetchPhase(context);
                throw ExceptionsHelper.convertToRuntime(e);
            }
            shardSearchStats.onFetchPhase(context, System.nanoTime() - time2);
            return new QueryFetchSearchResult(context.queryResult(), context.fetchResult());
        } catch (Throwable e) {
            logger.trace("Fetch phase failed", e);
            processFailure(context, e);
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            cleanContext(context);
        }
    }

    public ScrollQueryFetchSearchResult executeFetchPhase(InternalScrollSearchRequest request) {
        final SearchContext context = findContext(request.id());
        contextProcessing(context);
        try {
            ShardSearchStats shardSearchStats = context.indexShard().searchService();
            processScroll(request, context);
            shardSearchStats.onPreQueryPhase(context);
            long time = System.nanoTime();
            try {
                queryPhase.execute(context);
            } catch (Throwable e) {
                shardSearchStats.onFailedQueryPhase(context);
                throw ExceptionsHelper.convertToRuntime(e);
            }
            long time2 = System.nanoTime();
            shardSearchStats.onQueryPhase(context, time2 - time);
            shardSearchStats.onPreFetchPhase(context);
            try {
                shortcutDocIdsToLoad(context);
                fetchPhase.execute(context);
                if (fetchPhaseShouldFreeContext(context)) {
                    freeContext(request.id());
                } else {
                    contextProcessedSuccessfully(context);
                }
            } catch (Throwable e) {
                shardSearchStats.onFailedFetchPhase(context);
                throw ExceptionsHelper.convertToRuntime(e);
            }
            shardSearchStats.onFetchPhase(context, System.nanoTime() - time2);
            return new ScrollQueryFetchSearchResult(new QueryFetchSearchResult(context.queryResult(), context.fetchResult()), context.shardTarget());
        } catch (Throwable e) {
            logger.trace("Fetch phase failed", e);
            processFailure(context, e);
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            cleanContext(context);
        }
    }

    public FetchSearchResult executeFetchPhase(ShardFetchRequest request) {
        final SearchContext context = findContext(request.id());
        contextProcessing(context);
        final ShardSearchStats shardSearchStats = context.indexShard().searchService();
        try {
            if (request.lastEmittedDoc() != null) {
                context.scrollContext().lastEmittedDoc = request.lastEmittedDoc();
            }
            context.docIdsToLoad(request.docIds(), 0, request.docIdsSize());
            shardSearchStats.onPreFetchPhase(context);
            long time = System.nanoTime();
            fetchPhase.execute(context);
            if (fetchPhaseShouldFreeContext(context)) {
                freeContext(request.id());
            } else {
                contextProcessedSuccessfully(context);
            }
            shardSearchStats.onFetchPhase(context, System.nanoTime() - time);
            return context.fetchResult();
        } catch (Throwable e) {
            shardSearchStats.onFailedFetchPhase(context);
            logger.trace("Fetch phase failed", e);
            processFailure(context, e);
            throw ExceptionsHelper.convertToRuntime(e);
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

    final SearchContext createAndPutContext(ShardSearchRequest request) {
        SearchContext context = createContext(request, null);
        boolean success = false;
        try {
            putContext(context);
            if (request.scroll() != null) {
                context.indexShard().searchService().onNewScrollContext(context);
            }
            context.indexShard().searchService().onNewContext(context);
            success = true;
            return context;
        } finally {
            if (!success) {
                freeContext(context.id());
            }
        }
    }

    final SearchContext createContext(ShardSearchRequest request, @Nullable Engine.Searcher searcher) {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.getShard(request.shardId());

        SearchShardTarget shardTarget = new SearchShardTarget(clusterService.localNode().id(), indexShard.shardId().getIndex(), request.shardId());

        Engine.Searcher engineSearcher = searcher == null ? indexShard.acquireSearcher("search") : searcher;

        DefaultSearchContext context = new DefaultSearchContext(idGenerator.incrementAndGet(), request, shardTarget, engineSearcher, indexService, indexShard, scriptService, pageCacheRecycler, bigArrays, threadPool.estimatedTimeInMillisCounter(), parseFieldMatcher, defaultSearchTimeout);
        SearchContext.setCurrent(context);

        try {
            if (request.scroll() != null) {
                context.scrollContext(new ScrollContext());
                context.scrollContext().scroll = request.scroll();
            }
            if (request.template() != null) {
                ExecutableScript executable = this.scriptService.executable(request.template(), ScriptContext.Standard.SEARCH, Collections.emptyMap());
                BytesReference run = (BytesReference) executable.run();
                try (XContentParser parser = XContentFactory.xContent(run).createParser(run)) {
                    QueryParseContext queryParseContext = new QueryParseContext(indicesService.getIndicesQueryRegistry());
                    queryParseContext.reset(parser);
                    queryParseContext.parseFieldMatcher(parseFieldMatcher);
                    parseSource(context, SearchSourceBuilder.parseSearchSource(parser, queryParseContext));
                }
            }
            parseSource(context, request.source());

            // if the from and size are still not set, default them
            if (context.from() == -1) {
                context.from(0);
            }
            if (context.size() == -1) {
                context.size(10);
            }

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
        } catch (Throwable e) {
            context.close();
            throw ExceptionsHelper.convertToRuntime(e);
        }

        return context;
    }

    private void freeAllContextForIndex(Index index) {
        assert index != null;
        for (SearchContext ctx : activeContexts.values()) {
            if (index.equals(ctx.indexShard().shardId().getIndex())) {
                freeContext(ctx.id());
            }
        }
    }


    public boolean freeContext(long id) {
        final SearchContext context = removeContext(id);
        if (context != null) {
            try {
                context.indexShard().searchService().onFreeContext(context);
                if (context.scrollContext() != null) {
                    context.indexShard().searchService().onFreeScrollContext(context);
                }
            } finally {
                context.close();
            }
            return true;
        }
        return false;
    }

    public void freeAllScrollContexts() {
        for (SearchContext searchContext : activeContexts.values()) {
            if (searchContext.scrollContext() != null) {
                freeContext(searchContext.id());
            }
        }
    }

    private void contextProcessing(SearchContext context) {
        // disable timeout while executing a search
        context.accessed(-1);
    }

    private void contextProcessedSuccessfully(SearchContext context) {
        context.accessed(threadPool.estimatedTimeInMillis());
    }

    private void cleanContext(SearchContext context) {
        assert context == SearchContext.current();
        context.clearReleasables(Lifetime.PHASE);
        SearchContext.removeCurrent();
    }

    private void processFailure(SearchContext context, Throwable t) {
        freeContext(context.id());
        try {
            if (Lucene.isCorruptionException(t)) {
                context.indexShard().failShard("search execution corruption failure", t);
            }
        } catch (Throwable e) {
            logger.warn("failed to process shard failure to (potentially) send back shard failure on corruption", e);
        }
    }

    private void parseSource(DefaultSearchContext context, SearchSourceBuilder source) throws SearchContextException {
        // nothing to parse...
        if (source == null) {
            return;
        }
        QueryShardContext queryShardContext = context.getQueryShardContext();
        context.from(source.from());
        context.size(source.size());
        ObjectFloatHashMap<String> indexBoostMap = source.indexBoost();
        if (indexBoostMap != null) {
            Float indexBoost = indexBoostMap.get(context.shardTarget().index());
            if (indexBoost != null) {
                context.queryBoost(indexBoost);
            }
        }
        if (source.query() != null) {
            context.parsedQuery(queryShardContext.toQuery(source.query()));
        }
        if (source.postFilter() != null) {
            context.parsedPostFilter(queryShardContext.toQuery(source.postFilter()));
        }
        if (source.sorts() != null) {
            XContentParser completeSortParser = null;
            try {
                XContentBuilder completeSortBuilder = XContentFactory.jsonBuilder();
                completeSortBuilder.startObject();
                completeSortBuilder.startArray("sort");
                for (BytesReference sort : source.sorts()) {
                    XContentParser parser = XContentFactory.xContent(sort).createParser(sort);
                    parser.nextToken();
                    completeSortBuilder.copyCurrentStructure(parser);
                }
                completeSortBuilder.endArray();
                completeSortBuilder.endObject();
                BytesReference completeSortBytes = completeSortBuilder.bytes();
                completeSortParser = XContentFactory.xContent(completeSortBytes).createParser(completeSortBytes);
                completeSortParser.nextToken();
                completeSortParser.nextToken();
                completeSortParser.nextToken();
                this.elementParsers.get("sort").parse(completeSortParser, context);
            } catch (Exception e) {
                String sSource = "_na_";
                try {
                    sSource = source.toString();
                } catch (Throwable e1) {
                    // ignore
                }
                XContentLocation location = completeSortParser != null ? completeSortParser.getTokenLocation() : null;
                throw new SearchParseException(context, "failed to parse sort source [" + sSource + "]", location, e);
            }
        }
        context.trackScores(source.trackScores());
        if (source.minScore() != null) {
            context.minimumScore(source.minScore());
        }
        if (source.profile()) {
            context.setProfilers(new Profilers(context.searcher()));
        }
        context.timeoutInMillis(source.timeoutInMillis());
        context.terminateAfter(source.terminateAfter());
        if (source.aggregations() != null) {
            XContentParser completeAggregationsParser = null;
            try {
                XContentBuilder completeAggregationsBuilder = XContentFactory.jsonBuilder();
                completeAggregationsBuilder.startObject();
                for (BytesReference agg : source.aggregations()) {
                    XContentParser parser = XContentFactory.xContent(agg).createParser(agg);
                    parser.nextToken();
                    parser.nextToken();
                    completeAggregationsBuilder.field(parser.currentName());
                    parser.nextToken();
                    completeAggregationsBuilder.copyCurrentStructure(parser);
                }
                completeAggregationsBuilder.endObject();
                BytesReference completeAggregationsBytes = completeAggregationsBuilder.bytes();
                completeAggregationsParser = XContentFactory.xContent(completeAggregationsBytes).createParser(completeAggregationsBytes);
                completeAggregationsParser.nextToken();
                this.elementParsers.get("aggregations").parse(completeAggregationsParser, context);
            } catch (Exception e) {
                String sSource = "_na_";
                try {
                    sSource = source.toString();
                } catch (Throwable e1) {
                    // ignore
                }
                XContentLocation location = completeAggregationsParser != null ? completeAggregationsParser.getTokenLocation() : null;
                throw new SearchParseException(context, "failed to parse rescore source [" + sSource + "]", location, e);
            }
        }
        if (source.suggest() != null) {
            XContentParser suggestParser = null;
            try {
                suggestParser = XContentFactory.xContent(source.suggest()).createParser(source.suggest());
                suggestParser.nextToken();
                this.elementParsers.get("suggest").parse(suggestParser, context);
            } catch (Exception e) {
                String sSource = "_na_";
                try {
                    sSource = source.toString();
                } catch (Throwable e1) {
                    // ignore
                }
                XContentLocation location = suggestParser != null ? suggestParser.getTokenLocation() : null;
                throw new SearchParseException(context, "failed to parse suggest source [" + sSource + "]", location, e);
            }
        }
        if (source.rescores() != null) {
            try {
                for (RescoreBuilder<?> rescore : source.rescores()) {
                    context.addRescore(rescore.build(context.getQueryShardContext()));
                }
            } catch (IOException e) {
                throw new SearchContextException(context, "failed to create RescoreSearchContext", e);
            }
        }
        if (source.fields() != null) {
            context.fieldNames().addAll(source.fields());
        }
        if (source.explain() != null) {
            context.explain(source.explain());
        }
        if (source.fetchSource() != null) {
            context.fetchSourceContext(source.fetchSource());
        }
        if (source.fieldDataFields() != null) {
            FieldDataFieldsContext fieldDataFieldsContext = context.getFetchSubPhaseContext(FieldDataFieldsFetchSubPhase.CONTEXT_FACTORY);
            for (String field : source.fieldDataFields()) {
                fieldDataFieldsContext.add(new FieldDataField(field));
            }
            fieldDataFieldsContext.setHitExecutionNeeded(true);
        }
        if (source.highlighter() != null) {
            HighlightBuilder highlightBuilder = source.highlighter();
            try {
                context.highlight(highlightBuilder.build(context.getQueryShardContext()));
            } catch (IOException e) {
                throw new SearchContextException(context, "failed to create SearchContextHighlighter", e);
            }
        }
        if (source.innerHits() != null) {
            XContentParser innerHitsParser = null;
            try {
                innerHitsParser = XContentFactory.xContent(source.innerHits()).createParser(source.innerHits());
                innerHitsParser.nextToken();
                this.elementParsers.get("inner_hits").parse(innerHitsParser, context);
            } catch (Exception e) {
                String sSource = "_na_";
                try {
                    sSource = source.toString();
                } catch (Throwable e1) {
                    // ignore
                }
                XContentLocation location = innerHitsParser != null ? innerHitsParser.getTokenLocation() : null;
                throw new SearchParseException(context, "failed to parse suggest source [" + sSource + "]", location, e);
            }
        }
        if (source.scriptFields() != null) {
            for (org.elasticsearch.search.builder.SearchSourceBuilder.ScriptField field : source.scriptFields()) {
                SearchScript searchScript = context.scriptService().search(context.lookup(), field.script(), ScriptContext.Standard.SEARCH, Collections.emptyMap());
                context.scriptFields().add(new ScriptField(field.fieldName(), searchScript, field.ignoreFailure()));
            }
        }
        if (source.ext() != null) {
            XContentParser extParser = null;
            try {
                extParser = XContentFactory.xContent(source.ext()).createParser(source.ext());
                XContentParser.Token token = extParser.nextToken();
                String currentFieldName = null;
                while ((token = extParser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = extParser.currentName();
                    } else {
                        SearchParseElement parseElement = this.elementParsers.get(currentFieldName);
                        if (parseElement == null) {
                            throw new SearchParseException(context, "Unknown element [" + currentFieldName + "] in [ext]",
                                    extParser.getTokenLocation());
                        } else {
                            parseElement.parse(extParser, context);
                        }
                    }
                }
            } catch (Exception e) {
                String sSource = "_na_";
                try {
                    sSource = source.toString();
                } catch (Throwable e1) {
                    // ignore
                }
                XContentLocation location = extParser != null ? extParser.getTokenLocation() : null;
                throw new SearchParseException(context, "failed to parse ext source [" + sSource + "]", location, e);
            }
        }
        if (source.version() != null) {
            context.version(source.version());
        }
        if (source.stats() != null) {
            context.groupStats(source.stats());
        }
        if (source.searchAfter() != null && source.searchAfter().length > 0) {
            if (context.scrollContext() != null) {
                throw new SearchContextException(context, "`search_after` cannot be used in a scroll context.");
            }
            if (context.from() > 0) {
                throw new SearchContextException(context, "`from` parameter must be set to 0 when `search_after` is used.");
            }
            FieldDoc fieldDoc = SearchAfterBuilder.buildFieldDoc(context.sort(), source.searchAfter());
            context.searchAfter(fieldDoc);
        }
    }

    private static final int[] EMPTY_DOC_IDS = new int[0];

    /**
     * Shortcut ids to load, we load only "from" and up to "size". The phase controller
     * handles this as well since the result is always size * shards for Q_A_F
     */
    private void shortcutDocIdsToLoad(SearchContext context) {
        if (context.request().scroll() != null) {
            TopDocs topDocs = context.queryResult().topDocs();
            int[] docIdsToLoad = new int[topDocs.scoreDocs.length];
            for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                docIdsToLoad[i] = topDocs.scoreDocs[i].doc;
            }
            context.docIdsToLoad(docIdsToLoad, 0, docIdsToLoad.length);
        } else {
            TopDocs topDocs = context.queryResult().topDocs();
            if (topDocs.scoreDocs.length < context.from()) {
                // no more docs...
                context.docIdsToLoad(EMPTY_DOC_IDS, 0, 0);
                return;
            }
            int totalSize = context.from() + context.size();
            int[] docIdsToLoad = new int[Math.min(topDocs.scoreDocs.length - context.from(), context.size())];
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
        context.scrollContext().scroll = request.scroll();
        // update the context keep alive based on the new scroll value
        if (request.scroll() != null && request.scroll().keepAlive() != null) {
            context.keepAlive(request.scroll().keepAlive().millis());
        }
    }

    /**
     * Returns the number of active contexts in this
     * SearchService
     */
    public int getActiveContexts() {
        return this.activeContexts.size();
    }

    class Reaper implements Runnable {
        @Override
        public void run() {
            final long time = threadPool.estimatedTimeInMillis();
            for (SearchContext context : activeContexts.values()) {
                // Use the same value for both checks since lastAccessTime can
                // be modified by another thread between checks!
                final long lastAccessTime = context.lastAccessTime();
                if (lastAccessTime == -1L) { // its being processed or timeout is disabled
                    continue;
                }
                if ((time - lastAccessTime > context.keepAlive())) {
                    logger.debug("freeing search context [{}], time [{}], lastAccessTime [{}], keepAlive [{}]", context.id(), time, lastAccessTime, context.keepAlive());
                    freeContext(context.id());
                }
            }
        }
    }
}
