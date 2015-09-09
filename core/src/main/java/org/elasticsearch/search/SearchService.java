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

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.ObjectSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.ImmutableMap;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappedFieldType.Loading;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.query.TemplateQueryParser;
import org.elasticsearch.index.search.stats.ShardSearchStats;
import org.elasticsearch.index.search.stats.StatsGroupsParseElement;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.IndicesWarmer;
import org.elasticsearch.indices.IndicesWarmer.TerminationHandle;
import org.elasticsearch.indices.IndicesWarmer.WarmerContext;
import org.elasticsearch.indices.cache.request.IndicesRequestCache;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script.ScriptParseException;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.Template;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.search.dfs.DfsPhase;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ScrollQueryFetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchRequest;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SearchContext.Lifetime;
import org.elasticsearch.search.internal.ShardSearchLocalRequest;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.search.query.ScrollQuerySearchResult;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.Strings.hasLength;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

/**
 *
 */
public class SearchService extends AbstractLifecycleComponent<SearchService> {

    public static final String NORMS_LOADING_KEY = "index.norms.loading";
    public static final String DEFAULT_KEEPALIVE_KEY = "search.default_keep_alive";
    public static final String KEEPALIVE_INTERVAL_KEY = "search.keep_alive_interval";
    public static final String DEFAULT_SEARCH_TIMEOUT = "search.default_search_timeout";

    public static final TimeValue NO_TIMEOUT = timeValueMillis(-1);

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final IndicesWarmer indicesWarmer;

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

    private final ImmutableMap<String, SearchParseElement> elementParsers;

    private final ParseFieldMatcher parseFieldMatcher;

    @Inject
    public SearchService(Settings settings, NodeSettingsService nodeSettingsService, ClusterService clusterService, IndicesService indicesService,IndicesWarmer indicesWarmer, ThreadPool threadPool,
                         ScriptService scriptService, PageCacheRecycler pageCacheRecycler, BigArrays bigArrays, DfsPhase dfsPhase, QueryPhase queryPhase, FetchPhase fetchPhase,
                         IndicesRequestCache indicesQueryCache) {
        super(settings);
        this.parseFieldMatcher = new ParseFieldMatcher(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        indicesService.indicesLifecycle().addListener(new IndicesLifecycle.Listener() {
            @Override
            public void afterIndexClosed(Index index, @IndexSettings Settings indexSettings) {
                // once an index is closed we can just clean up all the pending search context information
                // to release memory and let references to the filesystem go etc.
                IndexMetaData idxMeta = SearchService.this.clusterService.state().metaData().index(index.getName());
                if (idxMeta != null && idxMeta.state() == IndexMetaData.State.CLOSE) {
                    // we need to check if it's really closed
                    // since sometimes due to a relocation we already closed the shard and that causes the index to be closed
                    // if we then close all the contexts we can get some search failures along the way which are not expected.
                    // it's fine to keep the contexts open if the index is still "alive"
                    // unfortunately we don't have a clear way to signal today why an index is closed.
                    afterIndexDeleted(index, indexSettings);
                }
            }

            @Override
            public void afterIndexDeleted(Index index, @IndexSettings Settings indexSettings) {
                freeAllContextForIndex(index);
            }
        });
        this.indicesWarmer = indicesWarmer;
        this.scriptService = scriptService;
        this.pageCacheRecycler = pageCacheRecycler;
        this.bigArrays = bigArrays;
        this.dfsPhase = dfsPhase;
        this.queryPhase = queryPhase;
        this.fetchPhase = fetchPhase;
        this.indicesQueryCache = indicesQueryCache;

        TimeValue keepAliveInterval = settings.getAsTime(KEEPALIVE_INTERVAL_KEY, timeValueMinutes(1));
        // we can have 5 minutes here, since we make sure to clean with search requests and when shard/index closes
        this.defaultKeepAlive = settings.getAsTime(DEFAULT_KEEPALIVE_KEY, timeValueMinutes(5)).millis();

        Map<String, SearchParseElement> elementParsers = new HashMap<>();
        elementParsers.putAll(dfsPhase.parseElements());
        elementParsers.putAll(queryPhase.parseElements());
        elementParsers.putAll(fetchPhase.parseElements());
        elementParsers.put("stats", new StatsGroupsParseElement());
        this.elementParsers = ImmutableMap.copyOf(elementParsers);

        this.keepAliveReaper = threadPool.scheduleWithFixedDelay(new Reaper(), keepAliveInterval);

        this.indicesWarmer.addListener(new NormsWarmer());
        this.indicesWarmer.addListener(new FieldDataWarmer());
        this.indicesWarmer.addListener(new SearchWarmer());

        defaultSearchTimeout = settings.getAsTime(DEFAULT_SEARCH_TIMEOUT, NO_TIMEOUT);
        nodeSettingsService.addListener(new SearchSettingsListener());
    }

    class SearchSettingsListener implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            final TimeValue maybeNewDefaultSearchTimeout = settings.getAsTime(SearchService.DEFAULT_SEARCH_TIMEOUT, SearchService.this.defaultSearchTimeout);
            if (!maybeNewDefaultSearchTimeout.equals(SearchService.this.defaultSearchTimeout)) {
                logger.info("updating [{}] from [{}] to [{}]", SearchService.DEFAULT_SEARCH_TIMEOUT, SearchService.this.defaultSearchTimeout, maybeNewDefaultSearchTimeout);
                SearchService.this.defaultSearchTimeout = maybeNewDefaultSearchTimeout;
            }
        }
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
        IndexShard indexShard = indexService.shardSafe(request.shardId());

        SearchShardTarget shardTarget = new SearchShardTarget(clusterService.localNode().id(), request.index(), request.shardId());

        Engine.Searcher engineSearcher = searcher == null ? indexShard.acquireSearcher("search") : searcher;

        SearchContext context = new DefaultSearchContext(idGenerator.incrementAndGet(), request, shardTarget, engineSearcher, indexService, indexShard, scriptService, pageCacheRecycler, bigArrays, threadPool.estimatedTimeInMillisCounter(), parseFieldMatcher, defaultSearchTimeout);
        SearchContext.setCurrent(context);
        try {
            if (request.scroll() != null) {
                context.scrollContext(new ScrollContext());
                context.scrollContext().scroll = request.scroll();
            }

            parseTemplate(request, context);
            parseSource(context, request.source());
            parseSource(context, request.extraSource());

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
            if (index.equals(ctx.indexShard().shardId().index())) {
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

    private void parseTemplate(ShardSearchRequest request, SearchContext searchContext) {

        BytesReference processedQuery;
        if (request.template() != null) {
            ExecutableScript executable = this.scriptService.executable(request.template(), ScriptContext.Standard.SEARCH, searchContext);
            processedQuery = (BytesReference) executable.run();
        } else {
            if (!hasLength(request.templateSource())) {
                return;
            }
            XContentParser parser = null;
            Template template = null;

            try {
                parser = XContentFactory.xContent(request.templateSource()).createParser(request.templateSource());
                template = TemplateQueryParser.parse(parser, searchContext.parseFieldMatcher(), "params", "template");

                if (template.getType() == ScriptService.ScriptType.INLINE) {
                    //Try to double parse for nested template id/file
                    parser = null;
                    try {
                        ExecutableScript executable = this.scriptService.executable(template, ScriptContext.Standard.SEARCH, searchContext);
                        processedQuery = (BytesReference) executable.run();
                        parser = XContentFactory.xContent(processedQuery).createParser(processedQuery);
                    } catch (ElasticsearchParseException epe) {
                        //This was an non-nested template, the parse failure was due to this, it is safe to assume this refers to a file
                        //for backwards compatibility and keep going
                        template = new Template(template.getScript(), ScriptService.ScriptType.FILE, MustacheScriptEngineService.NAME,
                                null, template.getParams());
                        ExecutableScript executable = this.scriptService.executable(template, ScriptContext.Standard.SEARCH, searchContext);
                        processedQuery = (BytesReference) executable.run();
                    }
                    if (parser != null) {
                        try {
                            Template innerTemplate = TemplateQueryParser.parse(parser, searchContext.parseFieldMatcher());
                            if (hasLength(innerTemplate.getScript()) && !innerTemplate.getType().equals(ScriptService.ScriptType.INLINE)) {
                                //An inner template referring to a filename or id
                                template = new Template(innerTemplate.getScript(), innerTemplate.getType(),
                                        MustacheScriptEngineService.NAME, null, template.getParams());
                                ExecutableScript executable = this.scriptService.executable(template, ScriptContext.Standard.SEARCH,
                                        searchContext);
                                processedQuery = (BytesReference) executable.run();
                            }
                        } catch (ScriptParseException e) {
                            // No inner template found, use original template from above
                        }
                    }
                } else {
                    ExecutableScript executable = this.scriptService.executable(template, ScriptContext.Standard.SEARCH, searchContext);
                    processedQuery = (BytesReference) executable.run();
                }
            } catch (IOException e) {
                throw new ElasticsearchParseException("Failed to parse template", e);
            } finally {
                Releasables.closeWhileHandlingException(parser);
            }

            if (!hasLength(template.getScript())) {
                throw new ElasticsearchParseException("Template must have [template] field configured");
            }
        }
        request.source(processedQuery);
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
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("failed to parse search source. source must be an object, but found [{}] instead", token.name());
            }
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    parser.nextToken();
                    SearchParseElement element = elementParsers.get(fieldName);
                    if (element == null) {
                        throw new SearchParseException(context, "failed to parse search source. unknown search element [" + fieldName + "]", parser.getTokenLocation());
                    }
                    element.parse(parser, context);
                } else {
                    if (token == null) {
                        throw new ElasticsearchParseException("failed to parse search source. end of query source reached but query is not complete.");
                    } else {
                        throw new ElasticsearchParseException("failed to parse search source. expected field name but got [{}]", token);
                    }
                }
            }
        } catch (Throwable e) {
            String sSource = "_na_";
            try {
                sSource = XContentHelper.convertToJson(source, false);
            } catch (Throwable e1) {
                // ignore
            }
            XContentLocation location = parser != null ? parser.getTokenLocation() : null;
            throw new SearchParseException(context, "failed to parse search source [" + sSource + "]", location, e);
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

    static class NormsWarmer extends IndicesWarmer.Listener {

        @Override
        public TerminationHandle warmNewReaders(final IndexShard indexShard, IndexMetaData indexMetaData, final WarmerContext context, ThreadPool threadPool) {
            final Loading defaultLoading = Loading.parse(indexMetaData.settings().get(NORMS_LOADING_KEY), Loading.LAZY);
            final MapperService mapperService = indexShard.mapperService();
            final ObjectSet<String> warmUp = new ObjectHashSet<>();
            for (DocumentMapper docMapper : mapperService.docMappers(false)) {
                for (FieldMapper fieldMapper : docMapper.mappers()) {
                    final String indexName = fieldMapper.fieldType().names().indexName();
                    Loading normsLoading = fieldMapper.fieldType().normsLoading();
                    if (normsLoading == null) {
                        normsLoading = defaultLoading;
                    }
                    if (fieldMapper.fieldType().indexOptions() != IndexOptions.NONE && !fieldMapper.fieldType().omitNorms() && normsLoading == Loading.EAGER) {
                        warmUp.add(indexName);
                    }
                }
            }

            final CountDownLatch latch = new CountDownLatch(1);
            // Norms loading may be I/O intensive but is not CPU intensive, so we execute it in a single task
            threadPool.executor(executor()).execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (ObjectCursor<String> stringObjectCursor : warmUp) {
                            final String indexName = stringObjectCursor.value;
                            final long start = System.nanoTime();
                            for (final LeafReaderContext ctx : context.searcher().reader().leaves()) {
                                final NumericDocValues values = ctx.reader().getNormValues(indexName);
                                if (values != null) {
                                    values.get(0);
                                }
                            }
                            if (indexShard.warmerService().logger().isTraceEnabled()) {
                                indexShard.warmerService().logger().trace("warmed norms for [{}], took [{}]", indexName, TimeValue.timeValueNanos(System.nanoTime() - start));
                            }
                        }
                    } catch (Throwable t) {
                        indexShard.warmerService().logger().warn("failed to warm-up norms", t);
                    } finally {
                        latch.countDown();
                    }
                }
            });

            return new TerminationHandle() {
                @Override
                public void awaitTermination() throws InterruptedException {
                    latch.await();
                }
            };
        }

        @Override
        public TerminationHandle warmTopReader(IndexShard indexShard, IndexMetaData indexMetaData, WarmerContext context, ThreadPool threadPool) {
            return TerminationHandle.NO_WAIT;
        }
    }

    static class FieldDataWarmer extends IndicesWarmer.Listener {

        @Override
        public TerminationHandle warmNewReaders(final IndexShard indexShard, IndexMetaData indexMetaData, final WarmerContext context, ThreadPool threadPool) {
            final MapperService mapperService = indexShard.mapperService();
            final Map<String, MappedFieldType> warmUp = new HashMap<>();
            for (DocumentMapper docMapper : mapperService.docMappers(false)) {
                for (FieldMapper fieldMapper : docMapper.mappers()) {
                    final FieldDataType fieldDataType;
                    final String indexName;
                    if (fieldMapper instanceof ParentFieldMapper) {
                        MappedFieldType joinFieldType = ((ParentFieldMapper) fieldMapper).getChildJoinFieldType();
                        if (joinFieldType == null) {
                            continue;
                        }
                        fieldDataType = joinFieldType.fieldDataType();
                        // TODO: this can be removed in 3.0 when the old parent/child impl is removed:
                        // related to: https://github.com/elastic/elasticsearch/pull/12418
                        indexName = fieldMapper.fieldType().names().indexName();
                    } else {
                        fieldDataType = fieldMapper.fieldType().fieldDataType();
                        indexName = fieldMapper.fieldType().names().indexName();
                    }

                    if (fieldDataType == null) {
                        continue;
                    }
                    if (fieldDataType.getLoading() == Loading.LAZY) {
                        continue;
                    }

                    if (warmUp.containsKey(indexName)) {
                        continue;
                    }
                    warmUp.put(indexName, fieldMapper.fieldType());
                }
            }
            final IndexFieldDataService indexFieldDataService = indexShard.indexFieldDataService();
            final Executor executor = threadPool.executor(executor());
            final CountDownLatch latch = new CountDownLatch(context.searcher().reader().leaves().size() * warmUp.size());
            for (final LeafReaderContext ctx : context.searcher().reader().leaves()) {
                for (final MappedFieldType fieldType : warmUp.values()) {
                    executor.execute(new Runnable() {

                        @Override
                        public void run() {
                            try {
                                final long start = System.nanoTime();
                                indexFieldDataService.getForField(fieldType).load(ctx);
                                if (indexShard.warmerService().logger().isTraceEnabled()) {
                                    indexShard.warmerService().logger().trace("warmed fielddata for [{}], took [{}]", fieldType.names().fullName(), TimeValue.timeValueNanos(System.nanoTime() - start));
                                }
                            } catch (Throwable t) {
                                indexShard.warmerService().logger().warn("failed to warm-up fielddata for [{}]", t, fieldType.names().fullName());
                            } finally {
                                latch.countDown();
                            }
                        }

                    });
                }
            }
            return new TerminationHandle() {
                @Override
                public void awaitTermination() throws InterruptedException {
                    latch.await();
                }
            };
        }

        @Override
        public TerminationHandle warmTopReader(final IndexShard indexShard, IndexMetaData indexMetaData, final WarmerContext context, ThreadPool threadPool) {
            final MapperService mapperService = indexShard.mapperService();
            final Map<String, MappedFieldType> warmUpGlobalOrdinals = new HashMap<>();
            for (DocumentMapper docMapper : mapperService.docMappers(false)) {
                for (FieldMapper fieldMapper : docMapper.mappers()) {
                    final FieldDataType fieldDataType;
                    final String indexName;
                    if (fieldMapper instanceof ParentFieldMapper) {
                        MappedFieldType joinFieldType = ((ParentFieldMapper) fieldMapper).getChildJoinFieldType();
                        if (joinFieldType == null) {
                            continue;
                        }
                        fieldDataType = joinFieldType.fieldDataType();
                        // TODO: this can be removed in 3.0 when the old parent/child impl is removed:
                        // related to: https://github.com/elastic/elasticsearch/pull/12418
                        indexName = fieldMapper.fieldType().names().indexName();
                    } else {
                        fieldDataType = fieldMapper.fieldType().fieldDataType();
                        indexName = fieldMapper.fieldType().names().indexName();
                    }
                    if (fieldDataType == null) {
                        continue;
                    }
                    if (fieldDataType.getLoading() != Loading.EAGER_GLOBAL_ORDINALS) {
                        continue;
                    }
                    if (warmUpGlobalOrdinals.containsKey(indexName)) {
                        continue;
                    }
                    warmUpGlobalOrdinals.put(indexName, fieldMapper.fieldType());
                }
            }
            final IndexFieldDataService indexFieldDataService = indexShard.indexFieldDataService();
            final Executor executor = threadPool.executor(executor());
            final CountDownLatch latch = new CountDownLatch(warmUpGlobalOrdinals.size());
            for (final MappedFieldType fieldType : warmUpGlobalOrdinals.values()) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            final long start = System.nanoTime();
                            IndexFieldData.Global ifd = indexFieldDataService.getForField(fieldType);
                            ifd.loadGlobal(context.reader());
                            if (indexShard.warmerService().logger().isTraceEnabled()) {
                                indexShard.warmerService().logger().trace("warmed global ordinals for [{}], took [{}]", fieldType.names().fullName(), TimeValue.timeValueNanos(System.nanoTime() - start));
                            }
                        } catch (Throwable t) {
                            indexShard.warmerService().logger().warn("failed to warm-up global ordinals for [{}]", t, fieldType.names().fullName());
                        } finally {
                            latch.countDown();
                        }
                    }
                });
            }
            return new TerminationHandle() {
                @Override
                public void awaitTermination() throws InterruptedException {
                    latch.await();
                }
            };
        }
    }

    class SearchWarmer extends IndicesWarmer.Listener {

        @Override
        public TerminationHandle warmNewReaders(IndexShard indexShard, IndexMetaData indexMetaData, WarmerContext context, ThreadPool threadPool) {
            return internalWarm(indexShard, indexMetaData, context, threadPool, false);
        }

        @Override
        public TerminationHandle warmTopReader(IndexShard indexShard, IndexMetaData indexMetaData, WarmerContext context, ThreadPool threadPool) {
            return internalWarm(indexShard, indexMetaData, context, threadPool, true);
        }

        public TerminationHandle internalWarm(final IndexShard indexShard, final IndexMetaData indexMetaData, final IndicesWarmer.WarmerContext warmerContext, ThreadPool threadPool, final boolean top) {
            IndexWarmersMetaData custom = indexMetaData.custom(IndexWarmersMetaData.TYPE);
            if (custom == null) {
                return TerminationHandle.NO_WAIT;
            }
            final Executor executor = threadPool.executor(executor());
            final CountDownLatch latch = new CountDownLatch(custom.entries().size());
            for (final IndexWarmersMetaData.Entry entry : custom.entries()) {
                executor.execute(new Runnable() {

                    @Override
                    public void run() {
                        SearchContext context = null;
                        try {
                            long now = System.nanoTime();
                            ShardSearchRequest request = new ShardSearchLocalRequest(indexShard.shardId(), indexMetaData.numberOfShards(),
                                    SearchType.QUERY_THEN_FETCH, entry.source(), entry.types(), entry.requestCache());
                            context = createContext(request, warmerContext.searcher());
                            // if we use sort, we need to do query to sort on it and load relevant field data
                            // if not, we might as well set size=0 (and cache if needed)
                            if (context.sort() == null) {
                                context.size(0);
                            }
                            boolean canCache = indicesQueryCache.canCache(request, context);
                            // early terminate when we can cache, since we can only do proper caching on top level searcher
                            // also, if we can't cache, and its top, we don't need to execute it, since we already did when its not top
                            if (canCache != top) {
                                return;
                            }
                            loadOrExecuteQueryPhase(request, context, queryPhase);
                            long took = System.nanoTime() - now;
                            if (indexShard.warmerService().logger().isTraceEnabled()) {
                                indexShard.warmerService().logger().trace("warmed [{}], took [{}]", entry.name(), TimeValue.timeValueNanos(took));
                            }
                        } catch (Throwable t) {
                            indexShard.warmerService().logger().warn("warmer [{}] failed", t, entry.name());
                        } finally {
                            try {
                                if (context != null) {
                                    freeContext(context.id());
                                    cleanContext(context);
                                }
                            } finally {
                                latch.countDown();
                            }
                        }
                    }

                });
            }
            return new TerminationHandle() {
                @Override
                public void awaitTermination() throws InterruptedException {
                    latch.await();
                }
            };
        }
    }

    class Reaper implements Runnable {
        @Override
        public void run() {
            final long time = threadPool.estimatedTimeInMillis();
            for (SearchContext context : activeContexts.values()) {
                // Use the same value for both checks since lastAccessTime can
                // be modified by another thread between checks!
                final long lastAccessTime = context.lastAccessTime();
                if (lastAccessTime == -1l) { // its being processed or timeout is disabled
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
