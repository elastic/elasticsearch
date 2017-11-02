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

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.InnerHitContextBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.elasticsearch.node.ResponseCollectorService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseContext;
import org.elasticsearch.search.dfs.DfsPhase;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ScrollQueryFetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchRequest;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsContext.ScriptField;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SearchContext.Lifetime;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.QueryPhaseExecutionException;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.ScrollQuerySearchResult;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.elasticsearch.common.unit.TimeValue.timeValueHours;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

public class SearchService extends AbstractLifecycleComponent implements IndexEventListener {

    // we can have 5 minutes here, since we make sure to clean with search requests and when shard/index closes
    public static final Setting<TimeValue> DEFAULT_KEEPALIVE_SETTING =
        Setting.positiveTimeSetting("search.default_keep_alive", timeValueMinutes(5), Property.NodeScope, Property.Dynamic);
    public static final Setting<TimeValue> MAX_KEEPALIVE_SETTING =
        Setting.positiveTimeSetting("search.max_keep_alive", timeValueHours(24), Property.NodeScope, Property.Dynamic);
    public static final Setting<TimeValue> KEEPALIVE_INTERVAL_SETTING =
        Setting.positiveTimeSetting("search.keep_alive_interval", timeValueMinutes(1), Property.NodeScope);
    /**
     * Enables low-level, frequent search cancellation checks. Enabling low-level checks will make long running searches to react
     * to the cancellation request faster. However, since it will produce more cancellation checks it might slow the search performance
     * down.
     */
    public static final Setting<Boolean> LOW_LEVEL_CANCELLATION_SETTING =
        Setting.boolSetting("search.low_level_cancellation", false, Property.Dynamic, Property.NodeScope);

    public static final TimeValue NO_TIMEOUT = timeValueMillis(-1);
    public static final Setting<TimeValue> DEFAULT_SEARCH_TIMEOUT_SETTING =
        Setting.timeSetting("search.default_search_timeout", NO_TIMEOUT, Property.Dynamic, Property.NodeScope);


    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final ScriptService scriptService;

    private final ResponseCollectorService responseCollectorService;

    private final BigArrays bigArrays;

    private final DfsPhase dfsPhase = new DfsPhase();

    private final QueryPhase queryPhase;

    private final FetchPhase fetchPhase;

    private volatile long defaultKeepAlive;

    private volatile long maxKeepAlive;

    private volatile TimeValue defaultSearchTimeout;

    private volatile boolean lowLevelCancellation;

    private final Cancellable keepAliveReaper;

    private final AtomicLong idGenerator = new AtomicLong();

    private final ConcurrentMapLong<SearchContext> activeContexts = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();

    public SearchService(ClusterService clusterService, IndicesService indicesService,
                         ThreadPool threadPool, ScriptService scriptService, BigArrays bigArrays, FetchPhase fetchPhase,
                         ResponseCollectorService responseCollectorService) {
        super(clusterService.getSettings());
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.scriptService = scriptService;
        this.responseCollectorService = responseCollectorService;
        this.bigArrays = bigArrays;
        this.queryPhase = new QueryPhase(settings);
        this.fetchPhase = fetchPhase;

        TimeValue keepAliveInterval = KEEPALIVE_INTERVAL_SETTING.get(settings);
        setKeepAlives(DEFAULT_KEEPALIVE_SETTING.get(settings), MAX_KEEPALIVE_SETTING.get(settings));

        clusterService.getClusterSettings().addSettingsUpdateConsumer(DEFAULT_KEEPALIVE_SETTING, MAX_KEEPALIVE_SETTING,
            this::setKeepAlives, this::validateKeepAlives);

        this.keepAliveReaper = threadPool.scheduleWithFixedDelay(new Reaper(), keepAliveInterval, Names.SAME);

        defaultSearchTimeout = DEFAULT_SEARCH_TIMEOUT_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DEFAULT_SEARCH_TIMEOUT_SETTING, this::setDefaultSearchTimeout);

        lowLevelCancellation = LOW_LEVEL_CANCELLATION_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(LOW_LEVEL_CANCELLATION_SETTING, this::setLowLevelCancellation);
    }

    private void validateKeepAlives(TimeValue defaultKeepAlive, TimeValue maxKeepAlive) {
        if (defaultKeepAlive.millis() > maxKeepAlive.millis()) {
            throw new IllegalArgumentException("Default keep alive setting for scroll [" + DEFAULT_KEEPALIVE_SETTING.getKey() + "]" +
                " should be smaller than max keep alive [" + MAX_KEEPALIVE_SETTING.getKey() + "], " +
                "was (" + defaultKeepAlive.format() + " > " + maxKeepAlive.format() + ")");
        }
    }

    private void setKeepAlives(TimeValue defaultKeepAlive, TimeValue maxKeepAlive) {
        validateKeepAlives(defaultKeepAlive, maxKeepAlive);
        this.defaultKeepAlive = defaultKeepAlive.millis();
        this.maxKeepAlive = maxKeepAlive.millis();
    }

    private void setDefaultSearchTimeout(TimeValue defaultSearchTimeout) {
        this.defaultSearchTimeout = defaultSearchTimeout;
    }

    private void setLowLevelCancellation(Boolean lowLevelCancellation) {
        this.lowLevelCancellation = lowLevelCancellation;
    }

    @Override
    public void afterIndexRemoved(Index index, IndexSettings indexSettings, IndexRemovalReason reason) {
        // once an index is removed due to deletion or closing, we can just clean up all the pending search context information
        // if we then close all the contexts we can get some search failures along the way which are not expected.
        // it's fine to keep the contexts open if the index is still "alive"
        // unfortunately we don't have a clear way to signal today why an index is closed.
        // to release memory and let references to the filesystem go etc.
        if (reason == IndexRemovalReason.DELETED || reason == IndexRemovalReason.CLOSED) {
            freeAllContextForIndex(index);
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
        keepAliveReaper.cancel();
    }

    public void executeDfsPhase(ShardSearchRequest request, SearchTask task, ActionListener<SearchPhaseResult> listener) {
        rewriteShardRequest(request, new ActionListener<ShardSearchRequest>() {
            @Override
            public void onResponse(ShardSearchRequest request) {
                try {
                    listener.onResponse(executeDfsPhase(request, task));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private DfsSearchResult executeDfsPhase(ShardSearchRequest request, SearchTask task) throws IOException {
        final SearchContext context = createAndPutContext(request);
        context.incRef();
        try {
            context.setTask(task);
            contextProcessing(context);
            dfsPhase.execute(context);
            contextProcessedSuccessfully(context);
            return context.dfsResult();
        } catch (Exception e) {
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
    private void loadOrExecuteQueryPhase(final ShardSearchRequest request, final SearchContext context) throws Exception {
        final boolean canCache = indicesService.canCache(request, context);
        context.getQueryShardContext().freezeContext();
        if (canCache) {
            indicesService.loadIntoContext(request, context, queryPhase);
        } else {
            queryPhase.execute(context);
        }
    }

    public void executeQueryPhase(ShardSearchRequest request, SearchTask task, ActionListener<SearchPhaseResult> listener) {
        rewriteShardRequest(request, new ActionListener<ShardSearchRequest>() {
            @Override
            public void onResponse(ShardSearchRequest request) {
                try {
                    listener.onResponse(executeQueryPhase(request, task));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    SearchPhaseResult executeQueryPhase(ShardSearchRequest request, SearchTask task) throws IOException {
        final SearchContext context = createAndPutContext(request);
        final SearchOperationListener operationListener = context.indexShard().getSearchOperationListener();
        context.incRef();
        boolean queryPhaseSuccess = false;
        try {
            context.setTask(task);
            operationListener.onPreQueryPhase(context);
            long time = System.nanoTime();
            contextProcessing(context);

            loadOrExecuteQueryPhase(request, context);

            if (context.queryResult().hasSearchContext() == false && context.scrollContext() == null) {
                freeContext(context.id());
            } else {
                contextProcessedSuccessfully(context);
            }
            final long afterQueryTime = System.nanoTime();
            queryPhaseSuccess = true;
            operationListener.onQueryPhase(context, afterQueryTime - time);
            if (request.numberOfShards() == 1) {
                return executeFetchPhase(context, operationListener, afterQueryTime);
            }
            return context.queryResult();
        } catch (Exception e) {
            // execution exception can happen while loading the cache, strip it
            if (e instanceof ExecutionException) {
                e = (e.getCause() == null || e.getCause() instanceof Exception) ?
                    (Exception) e.getCause() : new ElasticsearchException(e.getCause());
            }
            if (!queryPhaseSuccess) {
                operationListener.onFailedQueryPhase(context);
            }
            logger.trace("Query phase failed", e);
            processFailure(context, e);
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            cleanContext(context);
        }
    }

    private QueryFetchSearchResult executeFetchPhase(SearchContext context, SearchOperationListener operationListener,
                                                        long afterQueryTime) {
        operationListener.onPreFetchPhase(context);
        try {
            shortcutDocIdsToLoad(context);
            fetchPhase.execute(context);
            if (fetchPhaseShouldFreeContext(context)) {
                freeContext(context.id());
            } else {
                contextProcessedSuccessfully(context);
            }
        } catch (Exception e) {
            operationListener.onFailedFetchPhase(context);
            throw ExceptionsHelper.convertToRuntime(e);
        }
        operationListener.onFetchPhase(context, System.nanoTime() - afterQueryTime);
        return new QueryFetchSearchResult(context.queryResult(), context.fetchResult());
    }

    public ScrollQuerySearchResult executeQueryPhase(InternalScrollSearchRequest request, SearchTask task) {
        final SearchContext context = findContext(request.id(), request);
        SearchOperationListener operationListener = context.indexShard().getSearchOperationListener();
        context.incRef();
        try {
            context.setTask(task);
            operationListener.onPreQueryPhase(context);
            long time = System.nanoTime();
            contextProcessing(context);
            processScroll(request, context);
            queryPhase.execute(context);
            contextProcessedSuccessfully(context);
            operationListener.onQueryPhase(context, System.nanoTime() - time);
            return new ScrollQuerySearchResult(context.queryResult(), context.shardTarget());
        } catch (Exception e) {
            operationListener.onFailedQueryPhase(context);
            logger.trace("Query phase failed", e);
            processFailure(context, e);
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            cleanContext(context);
        }
    }

    public QuerySearchResult executeQueryPhase(QuerySearchRequest request, SearchTask task) {
        final SearchContext context = findContext(request.id(), request);
        context.setTask(task);
        IndexShard indexShard = context.indexShard();
        SearchOperationListener operationListener = indexShard.getSearchOperationListener();
        context.incRef();
        try {
            contextProcessing(context);
            context.searcher().setAggregatedDfs(request.dfs());

            operationListener.onPreQueryPhase(context);
            long time = System.nanoTime();
            queryPhase.execute(context);
            if (context.queryResult().hasSearchContext() == false && context.scrollContext() == null) {
                // no hits, we can release the context since there will be no fetch phase
                freeContext(context.id());
            } else {
                contextProcessedSuccessfully(context);
            }
            operationListener.onQueryPhase(context, System.nanoTime() - time);
            return context.queryResult();
        } catch (Exception e) {
            operationListener.onFailedQueryPhase(context);
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

    public ScrollQueryFetchSearchResult executeFetchPhase(InternalScrollSearchRequest request, SearchTask task) {
        final SearchContext context = findContext(request.id(), request);
        context.incRef();
        try {
            context.setTask(task);
            contextProcessing(context);
            SearchOperationListener operationListener = context.indexShard().getSearchOperationListener();
            processScroll(request, context);
            operationListener.onPreQueryPhase(context);
            final long time = System.nanoTime();
            try {
                queryPhase.execute(context);
            } catch (Exception e) {
                operationListener.onFailedQueryPhase(context);
                throw ExceptionsHelper.convertToRuntime(e);
            }
            long afterQueryTime = System.nanoTime();
            operationListener.onQueryPhase(context, afterQueryTime - time);
            QueryFetchSearchResult fetchSearchResult = executeFetchPhase(context, operationListener, afterQueryTime);

            return new ScrollQueryFetchSearchResult(fetchSearchResult,
                context.shardTarget());
        } catch (Exception e) {
            logger.trace("Fetch phase failed", e);
            processFailure(context, e);
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            cleanContext(context);
        }
    }

    public FetchSearchResult executeFetchPhase(ShardFetchRequest request, SearchTask task) {
        final SearchContext context = findContext(request.id(), request);
        final SearchOperationListener operationListener = context.indexShard().getSearchOperationListener();
        context.incRef();
        try {
            context.setTask(task);
            contextProcessing(context);
            if (request.lastEmittedDoc() != null) {
                context.scrollContext().lastEmittedDoc = request.lastEmittedDoc();
            }
            context.docIdsToLoad(request.docIds(), 0, request.docIdsSize());
            operationListener.onPreFetchPhase(context);
            long time = System.nanoTime();
            fetchPhase.execute(context);
            if (fetchPhaseShouldFreeContext(context)) {
                freeContext(request.id());
            } else {
                contextProcessedSuccessfully(context);
            }
            operationListener.onFetchPhase(context, System.nanoTime() - time);
            return context.fetchResult();
        } catch (Exception e) {
            operationListener.onFailedFetchPhase(context);
            logger.trace("Fetch phase failed", e);
            processFailure(context, e);
            throw ExceptionsHelper.convertToRuntime(e);
        } finally {
            cleanContext(context);
        }
    }

    private SearchContext findContext(long id, TransportRequest request) throws SearchContextMissingException {
        SearchContext context = activeContexts.get(id);
        if (context == null) {
            throw new SearchContextMissingException(id);
        }

        SearchOperationListener operationListener = context.indexShard().getSearchOperationListener();
        try {
            operationListener.validateSearchContext(context, request);
            return context;
        } catch (Exception e) {
            processFailure(context, e);
            throw e;
        }
    }

    final SearchContext createAndPutContext(ShardSearchRequest request) throws IOException {
        SearchContext context = createContext(request);
        boolean success = false;
        try {
            putContext(context);
            if (request.scroll() != null) {
                context.indexShard().getSearchOperationListener().onNewScrollContext(context);
            }
            context.indexShard().getSearchOperationListener().onNewContext(context);
            success = true;
            return context;
        } finally {
            if (!success) {
                freeContext(context.id());
            }
        }
    }

    final SearchContext createContext(ShardSearchRequest request) throws IOException {
        final DefaultSearchContext context = createSearchContext(request, defaultSearchTimeout);
        try {
            if (request.scroll() != null) {
                context.scrollContext(new ScrollContext());
                context.scrollContext().scroll = request.scroll();
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
            contextScrollKeepAlive(context, keepAlive);
            context.lowLevelCancellation(lowLevelCancellation);
        } catch (Exception e) {
            context.close();
            throw ExceptionsHelper.convertToRuntime(e);
        }

        return context;
    }

    public DefaultSearchContext createSearchContext(ShardSearchRequest request, TimeValue timeout)
        throws IOException {
        return createSearchContext(request, timeout, true);
    }
    private DefaultSearchContext createSearchContext(ShardSearchRequest request, TimeValue timeout,
                                                     boolean assertAsyncActions)
            throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(request.shardId().getId());
        SearchShardTarget shardTarget = new SearchShardTarget(clusterService.localNode().getId(),
                indexShard.shardId(), request.getClusterAlias(), OriginalIndices.NONE);
        Engine.Searcher engineSearcher = indexShard.acquireSearcher("search");

        final DefaultSearchContext searchContext = new DefaultSearchContext(idGenerator.incrementAndGet(), request, shardTarget,
            engineSearcher, indexService, indexShard, bigArrays, threadPool.estimatedTimeInMillisCounter(), timeout, fetchPhase,
            request.getClusterAlias());
        boolean success = false;
        try {
            // we clone the query shard context here just for rewriting otherwise we
            // might end up with incorrect state since we are using now() or script services
            // during rewrite and normalized / evaluate templates etc.
            QueryShardContext context = new QueryShardContext(searchContext.getQueryShardContext());
            Rewriteable.rewrite(request.getRewriteable(), context, assertAsyncActions);
            assert searchContext.getQueryShardContext().isCachable();
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(searchContext);
            }
        }
        return searchContext;
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
            assert context.refCount() > 0 : " refCount must be > 0: " + context.refCount();
            try {
                context.indexShard().getSearchOperationListener().onFreeContext(context);
                if (context.scrollContext() != null) {
                    context.indexShard().getSearchOperationListener().onFreeScrollContext(context);
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

    private void contextScrollKeepAlive(SearchContext context, long keepAlive) throws IOException {
        if (keepAlive > maxKeepAlive) {
            throw new QueryPhaseExecutionException(context,
                "Keep alive for scroll (" + TimeValue.timeValueMillis(keepAlive).format() + ") is too large. " +
                    "It must be less than (" + TimeValue.timeValueMillis(maxKeepAlive).format() + "). " +
                    "This limit can be set by changing the [" + MAX_KEEPALIVE_SETTING.getKey() + "] cluster level setting.");
        }
        context.keepAlive(keepAlive);
    }

    private void contextProcessing(SearchContext context) {
        // disable timeout while executing a search
        context.accessed(-1);
    }

    private void contextProcessedSuccessfully(SearchContext context) {
        context.accessed(threadPool.relativeTimeInMillis());
    }

    private void cleanContext(SearchContext context) {
        try {
            context.clearReleasables(Lifetime.PHASE);
            context.setTask(null);
        } finally {
            context.decRef();
        }
    }

    private void processFailure(SearchContext context, Exception e) {
        freeContext(context.id());
        try {
            if (Lucene.isCorruptionException(e)) {
                context.indexShard().failShard("search execution corruption failure", e);
            }
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.warn("failed to process shard failure to (potentially) send back shard failure on corruption", inner);
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
        Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        if (source.query() != null) {
            InnerHitContextBuilder.extractInnerHits(source.query(), innerHitBuilders);
            context.parsedQuery(queryShardContext.toQuery(source.query()));
        }
        if (source.postFilter() != null) {
            InnerHitContextBuilder.extractInnerHits(source.postFilter(), innerHitBuilders);
            context.parsedPostFilter(queryShardContext.toQuery(source.postFilter()));
        }
        if (innerHitBuilders.size() > 0) {
            for (Map.Entry<String, InnerHitContextBuilder> entry : innerHitBuilders.entrySet()) {
                try {
                    entry.getValue().build(context, context.innerHits());
                } catch (IOException e) {
                    throw new SearchContextException(context, "failed to build inner_hits", e);
                }
            }
        }
        if (source.sorts() != null) {
            try {
                Optional<SortAndFormats> optionalSort = SortBuilder.buildSort(source.sorts(), context.getQueryShardContext());
                if (optionalSort.isPresent()) {
                    context.sort(optionalSort.get());
                }
            } catch (IOException e) {
                throw new SearchContextException(context, "failed to create sort elements", e);
            }
        }
        context.trackScores(source.trackScores());
        if (source.trackTotalHits() == false && context.scrollContext() != null) {
            throw new SearchContextException(context, "disabling [track_total_hits] is not allowed in a scroll context");
        }
        context.trackTotalHits(source.trackTotalHits());
        if (source.minScore() != null) {
            context.minimumScore(source.minScore());
        }
        if (source.profile()) {
            context.setProfilers(new Profilers(context.searcher()));
        }
        if (source.timeout() != null) {
            context.timeout(source.timeout());
        }
        context.terminateAfter(source.terminateAfter());
        if (source.aggregations() != null) {
            try {
                AggregatorFactories factories = source.aggregations().build(context, null);
                context.aggregations(new SearchContextAggregations(factories));
            } catch (IOException e) {
                throw new AggregationInitializationException("Failed to create aggregators", e);
            }
        }
        if (source.suggest() != null) {
            try {
                context.suggest(source.suggest().build(queryShardContext));
            } catch (IOException e) {
                throw new SearchContextException(context, "failed to create SuggestionSearchContext", e);
            }
        }
        if (source.rescores() != null) {
            try {
                for (RescorerBuilder<?> rescore : source.rescores()) {
                    context.addRescore(rescore.buildContext(queryShardContext));
                }
            } catch (IOException e) {
                throw new SearchContextException(context, "failed to create RescoreSearchContext", e);
            }
        }
        if (source.explain() != null) {
            context.explain(source.explain());
        }
        if (source.fetchSource() != null) {
            context.fetchSourceContext(source.fetchSource());
        }
        if (source.docValueFields() != null) {
            int maxAllowedDocvalueFields = context.mapperService().getIndexSettings().getMaxDocvalueFields();
            if (source.docValueFields().size() > maxAllowedDocvalueFields) {
                throw new IllegalArgumentException(
                        "Trying to retrieve too many docvalue_fields. Must be less than or equal to: [" + maxAllowedDocvalueFields
                                + "] but was [" + source.docValueFields().size() + "]. This limit can be set by changing the ["
                                + IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.getKey() + "] index level setting.");
            }
            context.docValueFieldsContext(new DocValueFieldsContext(source.docValueFields()));
        }
        if (source.highlighter() != null) {
            HighlightBuilder highlightBuilder = source.highlighter();
            try {
                context.highlight(highlightBuilder.build(queryShardContext));
            } catch (IOException e) {
                throw new SearchContextException(context, "failed to create SearchContextHighlighter", e);
            }
        }
        if (source.scriptFields() != null) {
            int maxAllowedScriptFields = context.mapperService().getIndexSettings().getMaxScriptFields();
            if (source.scriptFields().size() > maxAllowedScriptFields) {
                throw new IllegalArgumentException(
                        "Trying to retrieve too many script_fields. Must be less than or equal to: [" + maxAllowedScriptFields
                                + "] but was [" + source.scriptFields().size() + "]. This limit can be set by changing the ["
                                + IndexSettings.MAX_SCRIPT_FIELDS_SETTING.getKey() + "] index level setting.");
            }
            for (org.elasticsearch.search.builder.SearchSourceBuilder.ScriptField field : source.scriptFields()) {
                SearchScript.Factory factory = scriptService.compile(field.script(), SearchScript.CONTEXT);
                SearchScript.LeafFactory searchScript = factory.newFactory(field.script().getParams(), context.lookup());
                context.scriptFields().add(new ScriptField(field.fieldName(), searchScript, field.ignoreFailure()));
            }
        }
        if (source.ext() != null) {
            for (SearchExtBuilder searchExtBuilder : source.ext()) {
                context.addSearchExt(searchExtBuilder);
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

        if (source.slice() != null) {
            if (context.scrollContext() == null) {
                throw new SearchContextException(context, "`slice` cannot be used outside of a scroll context");
            }
            context.sliceBuilder(source.slice());
        }

        if (source.storedFields() != null) {
            if (source.storedFields().fetchFields() == false) {
                if (context.version()) {
                    throw new SearchContextException(context, "`stored_fields` cannot be disabled if version is requested");
                }
                if (context.sourceRequested()) {
                    throw new SearchContextException(context, "`stored_fields` cannot be disabled if _source is requested");
                }
            }
            context.storedFieldsContext(source.storedFields());
        }

        if (source.collapse() != null) {
            final CollapseContext collapseContext = source.collapse().build(context);
            context.collapse(collapseContext);
        }
    }

    /**
     * Shortcut ids to load, we load only "from" and up to "size". The phase controller
     * handles this as well since the result is always size * shards for Q_A_F
     */
    private void shortcutDocIdsToLoad(SearchContext context) {
        final int[] docIdsToLoad;
        int docsOffset = 0;
        final Suggest suggest = context.queryResult().suggest();
        int numSuggestDocs = 0;
        final List<CompletionSuggestion> completionSuggestions;
        if (suggest != null && suggest.hasScoreDocs()) {
            completionSuggestions = suggest.filter(CompletionSuggestion.class);
            for (CompletionSuggestion completionSuggestion : completionSuggestions) {
                numSuggestDocs += completionSuggestion.getOptions().size();
            }
        } else {
            completionSuggestions = Collections.emptyList();
        }
        if (context.request().scroll() != null) {
            TopDocs topDocs = context.queryResult().topDocs();
            docIdsToLoad = new int[topDocs.scoreDocs.length + numSuggestDocs];
            for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                docIdsToLoad[docsOffset++] = topDocs.scoreDocs[i].doc;
            }
        } else {
            TopDocs topDocs = context.queryResult().topDocs();
            if (topDocs.scoreDocs.length < context.from()) {
                // no more docs...
                docIdsToLoad = new int[numSuggestDocs];
            } else {
                int totalSize = context.from() + context.size();
                docIdsToLoad = new int[Math.min(topDocs.scoreDocs.length - context.from(), context.size()) +
                    numSuggestDocs];
                for (int i = context.from(); i < Math.min(totalSize, topDocs.scoreDocs.length); i++) {
                    docIdsToLoad[docsOffset++] = topDocs.scoreDocs[i].doc;
                }
            }
        }
        for (CompletionSuggestion completionSuggestion : completionSuggestions) {
            for (CompletionSuggestion.Entry.Option option : completionSuggestion.getOptions()) {
                docIdsToLoad[docsOffset++] = option.getDoc().doc;
            }
        }
        context.docIdsToLoad(docIdsToLoad, 0, docIdsToLoad.length);
    }

    private void processScroll(InternalScrollSearchRequest request, SearchContext context) throws IOException {
        // process scroll
        context.from(context.from() + context.size());
        context.scrollContext().scroll = request.scroll();
        // update the context keep alive based on the new scroll value
        if (request.scroll() != null && request.scroll().keepAlive() != null) {
            contextScrollKeepAlive(context, request.scroll().keepAlive().millis());
        }
    }

    /**
     * Returns the number of active contexts in this
     * SearchService
     */
    public int getActiveContexts() {
        return this.activeContexts.size();
    }

    public ResponseCollectorService getResponseCollectorService() {
        return this.responseCollectorService;
    }

    class Reaper implements Runnable {
        @Override
        public void run() {
            final long time = threadPool.relativeTimeInMillis();
            for (SearchContext context : activeContexts.values()) {
                // Use the same value for both checks since lastAccessTime can
                // be modified by another thread between checks!
                final long lastAccessTime = context.lastAccessTime();
                if (lastAccessTime == -1L) { // its being processed or timeout is disabled
                    continue;
                }
                if ((time - lastAccessTime > context.keepAlive())) {
                    logger.debug("freeing search context [{}], time [{}], lastAccessTime [{}], keepAlive [{}]", context.id(), time,
                        lastAccessTime, context.keepAlive());
                    freeContext(context.id());
                }
            }
        }
    }

    public AliasFilter buildAliasFilter(ClusterState state, String index, String... expressions) {
        return indicesService.buildAliasFilter(state, index, expressions);
    }

    /**
     * This method does a very quick rewrite of the query and returns true if the query can potentially match any documents.
     * This method can have false positives while if it returns <code>false</code> the query won't match any documents on the current
     * shard.
     */
    public boolean canMatch(ShardSearchRequest request) throws IOException {
        assert request.searchType() == SearchType.QUERY_THEN_FETCH : "unexpected search type: " + request.searchType();
        try (DefaultSearchContext context = createSearchContext(request, defaultSearchTimeout, false)) {
            SearchSourceBuilder source = context.request().source();
            if (canRewriteToMatchNone(source)) {
                QueryBuilder queryBuilder = source.query();
                return queryBuilder instanceof MatchNoneQueryBuilder == false;
            }
            return true; // null query means match_all
        }
    }

    /**
     * Returns true iff the given search source builder can be early terminated by rewriting to a match none query. Or in other words
     * if the execution of a the search request can be early terminated without executing it. This is for instance not possible if
     * a global aggregation is part of this request or if there is a suggest builder present.
     */
    public static boolean canRewriteToMatchNone(SearchSourceBuilder source) {
        if (source == null || source.query() == null || source.query() instanceof MatchAllQueryBuilder || source.suggest() != null) {
            return false;
        } else {
            AggregatorFactories.Builder aggregations = source.aggregations();
            if (aggregations != null) {
                if (aggregations.mustVisitAllDocs()) {
                    return false;
                }
            }
        }
        return true;
    }

    /*
     * Rewrites the search request with a light weight rewrite context in order to fetch resources asynchronously
     * The action listener is guaranteed to be executed on the search thread-pool
     */
    private void rewriteShardRequest(ShardSearchRequest request, ActionListener<ShardSearchRequest> listener) {
        // we also do rewrite on the coordinating node (TransportSearchService) but we also need to do it here for BWC as well as
        // AliasFilters that might need to be rewritten. These are edge-cases but we are every efficient doing the rewrite here so it's not
        // adding a lot of overhead
        Rewriteable.rewriteAndFetch(request.getRewriteable(), indicesService.getRewriteContext(request::nowInMillis),
            ActionListener.wrap(r ->
                    threadPool.executor(Names.SEARCH).execute(new AbstractRunnable() {
                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }

                        @Override
                        protected void doRun() throws Exception {
                            listener.onResponse(request);
                        }
                    }), listener::onFailure));
    }

    /**
     * Returns a new {@link QueryRewriteContext} with the given <tt>now</tt> provider
     */
    public QueryRewriteContext getRewriteContext(LongSupplier nowInMillis) {
        return indicesService.getRewriteContext(nowInMillis);
    }
}
