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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.core.internal.io.IOUtils;
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
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.elasticsearch.node.ResponseCollectorService;
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
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
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.ScrollQuerySearchResult;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.MinAndMax;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.common.unit.TimeValue.timeValueHours;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

public class SearchService extends AbstractLifecycleComponent implements IndexEventListener {
    private static final Logger logger = LogManager.getLogger(SearchService.class);

    // we can have 5 minutes here, since we make sure to clean with search requests and when shard/index closes
    public static final Setting<TimeValue> DEFAULT_KEEPALIVE_SETTING =
        Setting.positiveTimeSetting("search.default_keep_alive", timeValueMinutes(5), Property.NodeScope, Property.Dynamic);
    public static final Setting<TimeValue> MAX_KEEPALIVE_SETTING =
        Setting.positiveTimeSetting("search.max_keep_alive", timeValueHours(24), Property.NodeScope, Property.Dynamic);
    public static final Setting<TimeValue> KEEPALIVE_INTERVAL_SETTING =
        Setting.positiveTimeSetting("search.keep_alive_interval", timeValueMinutes(1), Property.NodeScope);

    /**
     * Enables low-level, frequent search cancellation checks. Enabling low-level checks will make long running searches to react
     * to the cancellation request faster. It will produce more cancellation checks but benchmarking has shown these did not
     * noticeably slow down searches.
     */
    public static final Setting<Boolean> LOW_LEVEL_CANCELLATION_SETTING =
        Setting.boolSetting("search.low_level_cancellation", true, Property.Dynamic, Property.NodeScope);

    public static final TimeValue NO_TIMEOUT = timeValueMillis(-1);
    public static final Setting<TimeValue> DEFAULT_SEARCH_TIMEOUT_SETTING =
        Setting.timeSetting("search.default_search_timeout", NO_TIMEOUT, Property.Dynamic, Property.NodeScope);
    public static final Setting<Boolean> DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS =
            Setting.boolSetting("search.default_allow_partial_results", true, Property.Dynamic, Property.NodeScope);

    public static final Setting<Integer> MAX_OPEN_SCROLL_CONTEXT =
        Setting.intSetting("search.max_open_scroll_context", 500, 0, Property.Dynamic, Property.NodeScope);

    public static final int DEFAULT_SIZE = 10;
    public static final int DEFAULT_FROM = 0;

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

    private volatile boolean defaultAllowPartialSearchResults;

    private volatile boolean lowLevelCancellation;

    private volatile int maxOpenScrollContext;

    private final Cancellable keepAliveReaper;

    private final AtomicLong idGenerator = new AtomicLong();

    private final ConcurrentMapLong<SearchContext> activeContexts = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();

    private final MultiBucketConsumerService multiBucketConsumerService;

    private final AtomicInteger openScrollContexts = new AtomicInteger();

    public SearchService(ClusterService clusterService, IndicesService indicesService,
                         ThreadPool threadPool, ScriptService scriptService, BigArrays bigArrays, FetchPhase fetchPhase,
                         ResponseCollectorService responseCollectorService, CircuitBreakerService circuitBreakerService) {
        Settings settings = clusterService.getSettings();
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.scriptService = scriptService;
        this.responseCollectorService = responseCollectorService;
        this.bigArrays = bigArrays;
        this.queryPhase = new QueryPhase();
        this.fetchPhase = fetchPhase;
        this.multiBucketConsumerService = new MultiBucketConsumerService(clusterService, settings,
            circuitBreakerService.getBreaker(CircuitBreaker.REQUEST));

        TimeValue keepAliveInterval = KEEPALIVE_INTERVAL_SETTING.get(settings);
        setKeepAlives(DEFAULT_KEEPALIVE_SETTING.get(settings), MAX_KEEPALIVE_SETTING.get(settings));

        clusterService.getClusterSettings().addSettingsUpdateConsumer(DEFAULT_KEEPALIVE_SETTING, MAX_KEEPALIVE_SETTING,
            this::setKeepAlives, this::validateKeepAlives);

        this.keepAliveReaper = threadPool.scheduleWithFixedDelay(new Reaper(), keepAliveInterval, Names.SAME);

        defaultSearchTimeout = DEFAULT_SEARCH_TIMEOUT_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DEFAULT_SEARCH_TIMEOUT_SETTING, this::setDefaultSearchTimeout);

        defaultAllowPartialSearchResults = DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS,
                this::setDefaultAllowPartialSearchResults);

        maxOpenScrollContext = MAX_OPEN_SCROLL_CONTEXT.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_OPEN_SCROLL_CONTEXT, this::setMaxOpenScrollContext);

        lowLevelCancellation = LOW_LEVEL_CANCELLATION_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(LOW_LEVEL_CANCELLATION_SETTING, this::setLowLevelCancellation);
    }

    private void validateKeepAlives(TimeValue defaultKeepAlive, TimeValue maxKeepAlive) {
        if (defaultKeepAlive.millis() > maxKeepAlive.millis()) {
            throw new IllegalArgumentException("Default keep alive setting for scroll [" + DEFAULT_KEEPALIVE_SETTING.getKey() + "]" +
                " should be smaller than max keep alive [" + MAX_KEEPALIVE_SETTING.getKey() + "], " +
                "was (" + defaultKeepAlive + " > " + maxKeepAlive + ")");
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

    private void setDefaultAllowPartialSearchResults(boolean defaultAllowPartialSearchResults) {
        this.defaultAllowPartialSearchResults = defaultAllowPartialSearchResults;
    }

    public boolean defaultAllowPartialSearchResults() {
        return defaultAllowPartialSearchResults;
    }

    private void setMaxOpenScrollContext(int maxOpenScrollContext) {
        this.maxOpenScrollContext = maxOpenScrollContext;
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
        if (reason == IndexRemovalReason.DELETED || reason == IndexRemovalReason.CLOSED || reason == IndexRemovalReason.REOPENED) {
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

    public void executeDfsPhase(ShardSearchRequest request, SearchShardTask task, ActionListener<SearchPhaseResult> listener) {
        rewriteShardRequest(request, ActionListener.wrap(
            // fork the execution in the search thread pool and wraps the searcher
            // to execute the query
            context -> {
                try {
                    context.wrapSearcher().execute(() -> {
                        final SearchPhaseResult result;
                        try {
                            result = executeDfsPhase(context, task);
                        } catch (Exception exc) {
                            listener.onFailure(exc);
                            return;
                        }
                        listener.onResponse(result);
                    });
                } catch (Exception exc) {
                    // if the execution is rejected we need to close the searcher
                    IOUtils.closeWhileHandlingException(context.searcher);
                    listener.onFailure(exc);
                }
            }, listener::onFailure));
    }

    private DfsSearchResult executeDfsPhase(SearchRewriteContext rewriteContext, SearchShardTask task) throws IOException {
        final SearchContext context = createAndPutContext(rewriteContext);
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
            throw e;
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

    public void executeQueryPhase(ShardSearchRequest request, SearchShardTask task, ActionListener<SearchPhaseResult> listener) {
        assert request.canReturnNullResponseIfMatchNoDocs() == false || request.numberOfShards() > 1
            : "empty responses require more than one shard";
        rewriteShardRequest(request, ActionListener.wrap(
            context -> {
                try {
                    ShardSearchRequest rewritten = context.request;
                    if (rewritten.canReturnNullResponseIfMatchNoDocs()
                            && canRewriteToMatchNone(rewritten.source())
                            && rewritten.source().query() instanceof MatchNoneQueryBuilder) {
                        assert request.scroll() == null : "must always create search context for scroll requests";
                        onMatchNoDocs(context, listener);
                    } else {
                        // fork the execution in the search thread pool and wraps the searcher
                        // to execute the query
                        context.wrapSearcher().execute(() -> {
                            final SearchPhaseResult result;
                            try {
                                result = executeQueryPhase(context, task);
                            } catch (Exception exc) {
                                listener.onFailure(exc);
                                return;
                            }
                            listener.onResponse(result);
                        });
                    }
                } catch (Exception exc) {
                    // if the execution is rejected we need to close the searcher
                    IOUtils.closeWhileHandlingException(context.searcher);
                    listener.onFailure(exc);
                }
            }, listener::onFailure));
    }

    private void onMatchNoDocs(SearchRewriteContext rewriteContext, ActionListener<SearchPhaseResult> listener) {
        // creates a lightweight search context that we use to inform context listeners
        // before closing
        SearchContext searchContext = createSearchContext(rewriteContext, defaultSearchTimeout);
        try (searchContext) {
            onNewContext(searchContext);
            onFreeContext(searchContext);
        } catch (Exception exc) {
            listener.onFailure(exc);
            return;
        }
        listener.onResponse(QuerySearchResult.nullInstance());
    }

    private <T> void runAsync(long id, Supplier<T> executable, ActionListener<T> listener) {
        getExecutor(id).execute(ActionRunnable.supply(listener, executable::get));
    }

    private SearchPhaseResult executeQueryPhase(SearchRewriteContext rewriteContext, SearchShardTask task) throws Exception {
        final SearchContext context = createAndPutContext(rewriteContext);
        final ShardSearchRequest request = rewriteContext.request;
        context.incRef();
        try {
            context.setTask(task);
            final long afterQueryTime;
            try (SearchOperationListenerExecutor executor = new SearchOperationListenerExecutor(context)) {
                contextProcessing(context);
                loadOrExecuteQueryPhase(request, context);
                if (context.queryResult().hasSearchContext() == false && context.scrollContext() == null) {
                    freeContext(context.id());
                } else {
                    contextProcessedSuccessfully(context);
                }
                afterQueryTime = executor.success();
            }
            if (request.numberOfShards() == 1) {
                return executeFetchPhase(context, afterQueryTime);
            }
            return context.queryResult();
        } catch (Exception e) {
            // execution exception can happen while loading the cache, strip it
            if (e instanceof ExecutionException) {
                e = (e.getCause() == null || e.getCause() instanceof Exception) ?
                    (Exception) e.getCause() : new ElasticsearchException(e.getCause());
            }
            logger.trace("Query phase failed", e);
            processFailure(context, e);
            throw e;
        } finally {
            cleanContext(context);
        }
    }

    private QueryFetchSearchResult executeFetchPhase(SearchContext context, long afterQueryTime) {
        try (SearchOperationListenerExecutor executor = new SearchOperationListenerExecutor(context, true, afterQueryTime)){
            shortcutDocIdsToLoad(context);
            fetchPhase.execute(context);
            if (fetchPhaseShouldFreeContext(context)) {
                freeContext(context.id());
            } else {
                contextProcessedSuccessfully(context);
            }
            executor.success();
        }
        return new QueryFetchSearchResult(context.queryResult(), context.fetchResult());
    }

    public void executeQueryPhase(InternalScrollSearchRequest request,
                                  SearchShardTask task,
                                  ActionListener<ScrollQuerySearchResult> listener) {
        runAsync(request.id(), () -> {
            final SearchContext context = findContext(request.id(), request);
            context.incRef();
            try (SearchOperationListenerExecutor executor = new SearchOperationListenerExecutor(context)) {
                context.setTask(task);
                contextProcessing(context);
                processScroll(request, context);
                queryPhase.execute(context);
                contextProcessedSuccessfully(context);
                executor.success();
                return new ScrollQuerySearchResult(context.queryResult(), context.shardTarget());
            } catch (Exception e) {
                logger.trace("Query phase failed", e);
                processFailure(context, e);
                throw e;
            } finally {
                cleanContext(context);
            }
        }, listener);
    }

    public void executeQueryPhase(QuerySearchRequest request, SearchShardTask task, ActionListener<QuerySearchResult> listener) {
        runAsync(request.id(), () -> {
            final SearchContext context = findContext(request.id(), request);
            context.setTask(task);
            context.incRef();
            try (SearchOperationListenerExecutor executor = new SearchOperationListenerExecutor(context)) {
                contextProcessing(context);
                context.searcher().setAggregatedDfs(request.dfs());
                queryPhase.execute(context);
                if (context.queryResult().hasSearchContext() == false && context.scrollContext() == null) {
                    // no hits, we can release the context since there will be no fetch phase
                    freeContext(context.id());
                } else {
                    contextProcessedSuccessfully(context);
                }
                executor.success();
                return context.queryResult();
            } catch (Exception e) {
                logger.trace("Query phase failed", e);
                processFailure(context, e);
                throw e;
            } finally {
                cleanContext(context);
            }
        }, listener);
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

    final Executor getExecutor(long id) {
        SearchContext context = activeContexts.get(id);
        if (context == null) {
            throw new SearchContextMissingException(id);
        }
        return getExecutor(context.indexShard());
    }

    private Executor getExecutor(IndexShard indexShard) {
        assert indexShard != null;
        return threadPool.executor(indexShard.indexSettings().isSearchThrottled() ? Names.SEARCH_THROTTLED : Names.SEARCH);
    }

    public void executeFetchPhase(InternalScrollSearchRequest request, SearchShardTask task,
                                  ActionListener<ScrollQueryFetchSearchResult> listener) {
        runAsync(request.id(), () -> {
            final SearchContext context = findContext(request.id(), request);
            context.setTask(task);
            context.incRef();
            try (SearchOperationListenerExecutor executor = new SearchOperationListenerExecutor(context)){
                contextProcessing(context);
                processScroll(request, context);
                queryPhase.execute(context);
                final long afterQueryTime = executor.success();
                QueryFetchSearchResult fetchSearchResult = executeFetchPhase(context, afterQueryTime);
                return new ScrollQueryFetchSearchResult(fetchSearchResult, context.shardTarget());
            } catch (Exception e) {
                logger.trace("Fetch phase failed", e);
                processFailure(context, e);
                throw e;
            } finally {
                cleanContext(context);
            }
        }, listener);
    }

    public void executeFetchPhase(ShardFetchRequest request, SearchShardTask task, ActionListener<FetchSearchResult> listener) {
        runAsync(request.id(), () -> {
            final SearchContext context = findContext(request.id(), request);
            context.incRef();
            try {
                context.setTask(task);
                contextProcessing(context);
                if (request.lastEmittedDoc() != null) {
                    context.scrollContext().lastEmittedDoc = request.lastEmittedDoc();
                }
                context.docIdsToLoad(request.docIds(), 0, request.docIdsSize());
                try (SearchOperationListenerExecutor executor = new SearchOperationListenerExecutor(context, true, System.nanoTime())) {
                    fetchPhase.execute(context);
                    if (fetchPhaseShouldFreeContext(context)) {
                        freeContext(request.id());
                    } else {
                        contextProcessedSuccessfully(context);
                    }
                    executor.success();
                }
                return context.fetchResult();
            } catch (Exception e) {
                logger.trace("Fetch phase failed", e);
                processFailure(context, e);
                throw e;
            } finally {
                cleanContext(context);
            }
        }, listener);
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

    final SearchContext createAndPutContext(SearchRewriteContext rewriteContext) {
        SearchContext context = createContext(rewriteContext);
        onNewContext(context);
        boolean success = false;
        try {
            putContext(context);
            success = true;
            return context;
        } finally {
            if (success == false) {
                freeContext(context.id());
            }
        }
    }

    private void onNewContext(SearchContext context) {
        boolean success = false;
        try {
            if (context.scrollContext() != null) {
                openScrollContexts.incrementAndGet();
                context.indexShard().getSearchOperationListener().onNewScrollContext(context);
            }
            context.indexShard().getSearchOperationListener().onNewContext(context);
            success = true;
        } finally {
            // currently, the concrete listener is CompositeListener, which swallows exceptions, but here we anyway try to do the
            // right thing by closing and notifying onFreeXXX in case one of the listeners fails with an exception in the future.
            if (success == false) {
                try (context) {
                    onFreeContext(context);
                }
            }
        }
    }

    final SearchContext createContext(SearchRewriteContext rewriteContext) {
        final DefaultSearchContext context = createSearchContext(rewriteContext, defaultSearchTimeout);
        try {
            if (rewriteContext.request != null && openScrollContexts.get() >= maxOpenScrollContext) {
                throw new ElasticsearchException(
                    "Trying to create too many scroll contexts. Must be less than or equal to: [" +
                        maxOpenScrollContext + "]. " + "This limit can be set by changing the ["
                        + MAX_OPEN_SCROLL_CONTEXT.getKey() + "] setting.");
            }
            final ShardSearchRequest request = rewriteContext.request;
            if (request.scroll() != null) {
                context.scrollContext(new ScrollContext());
                context.scrollContext().scroll = request.scroll();
            }
            parseSource(context, request.source());

            // if the from and size are still not set, default them
            if (context.from() == -1) {
                context.from(DEFAULT_FROM);
            }
            if (context.size() == -1) {
                context.size(DEFAULT_SIZE);
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
            throw e;
        }

        return context;
    }

    public DefaultSearchContext createSearchContext(ShardSearchRequest request, TimeValue timeout) throws IOException {
        IndexShard shard = indicesService.indexServiceSafe(request.shardId().getIndex()).getShard(request.shardId().id());
        SearchRewriteContext rewriteContext = acquireSearcherAndRewrite(request, shard);
        // make sure that we wrap the searcher when executing the query
        return createSearchContext(rewriteContext.wrapSearcher(), timeout);
    }

    private DefaultSearchContext createSearchContext(SearchRewriteContext rewriteContext, TimeValue timeout) {
        boolean success = false;
        try {
            final ShardSearchRequest request = rewriteContext.request;
            final Engine.Searcher searcher = rewriteContext.searcher;
            IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
            IndexShard indexShard = indexService.getShard(request.shardId().getId());
            SearchShardTarget shardTarget = new SearchShardTarget(clusterService.localNode().getId(),
                indexShard.shardId(), request.getClusterAlias(), OriginalIndices.NONE);
            DefaultSearchContext searchContext = new DefaultSearchContext(idGenerator.incrementAndGet(), request, shardTarget,
                searcher, clusterService, indexService, indexShard, bigArrays, threadPool::relativeTimeInMillis, timeout, fetchPhase);
            success = true;
            return searchContext;
        } finally {
            if (success == false) {
                // we handle the case where `IndicesService#indexServiceSafe`or `IndexService#getShard`, or the DefaultSearchContext
                // constructor throws an exception since we would otherwise leak a searcher and this can have severe implications
                // (unable to obtain shard lock exceptions).
                IOUtils.closeWhileHandlingException(rewriteContext.searcher);
            }
        }
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
        try (SearchContext context = removeContext(id)) {
            if (context != null) {
                onFreeContext(context);
                return true;
            }
            return false;
        }
    }

    private void onFreeContext(SearchContext context) {
        assert context.refCount() > 0 : " refCount must be > 0: " + context.refCount();
        assert activeContexts.containsKey(context.id()) == false;
        context.indexShard().getSearchOperationListener().onFreeContext(context);
        if (context.scrollContext() != null) {
            openScrollContexts.decrementAndGet();
            context.indexShard().getSearchOperationListener().onFreeScrollContext(context);
        }
    }

    public void freeAllScrollContexts() {
        for (SearchContext searchContext : activeContexts.values()) {
            if (searchContext.scrollContext() != null) {
                freeContext(searchContext.id());
            }
        }
    }

    private void contextScrollKeepAlive(SearchContext context, long keepAlive) {
        if (keepAlive > maxKeepAlive) {
            throw new IllegalArgumentException(
                "Keep alive for scroll (" + TimeValue.timeValueMillis(keepAlive) + ") is too large. " +
                    "It must be less than (" + TimeValue.timeValueMillis(maxKeepAlive) + "). " +
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

    private void parseSource(DefaultSearchContext context, SearchSourceBuilder source) throws SearchException {
        // nothing to parse...
        if (source == null) {
            return;
        }
        SearchShardTarget shardTarget = context.shardTarget();
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
                    throw new SearchException(shardTarget, "failed to build inner_hits", e);
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
                throw new SearchException(shardTarget, "failed to create sort elements", e);
            }
        }
        context.trackScores(source.trackScores());
        if (source.trackTotalHitsUpTo() != null
                && source.trackTotalHitsUpTo() != SearchContext.TRACK_TOTAL_HITS_ACCURATE
                && context.scrollContext() != null) {
            throw new SearchException(shardTarget, "disabling [track_total_hits] is not allowed in a scroll context");
        }
        if (source.trackTotalHitsUpTo() != null) {
            context.trackTotalHitsUpTo(source.trackTotalHitsUpTo());
        }
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
                AggregatorFactories factories = source.aggregations().build(queryShardContext, null);
                context.aggregations(new SearchContextAggregations(factories, multiBucketConsumerService.create()));
            } catch (IOException e) {
                throw new AggregationInitializationException("Failed to create aggregators", e);
            }
        }
        if (source.suggest() != null) {
            try {
                context.suggest(source.suggest().build(queryShardContext));
            } catch (IOException e) {
                throw new SearchException(shardTarget, "failed to create SuggestionSearchContext", e);
            }
        }
        if (source.rescores() != null) {
            try {
                for (RescorerBuilder<?> rescore : source.rescores()) {
                    context.addRescore(rescore.buildContext(queryShardContext));
                }
            } catch (IOException e) {
                throw new SearchException(shardTarget, "failed to create RescoreSearchContext", e);
            }
        }
        if (source.explain() != null) {
            context.explain(source.explain());
        }
        if (source.fetchSource() != null) {
            context.fetchSourceContext(source.fetchSource());
        }
        if (source.docValueFields() != null) {
            List<DocValueFieldsContext.FieldAndFormat> docValueFields = new ArrayList<>();
            for (DocValueFieldsContext.FieldAndFormat format : source.docValueFields()) {
                Collection<String> fieldNames = context.mapperService().simpleMatchToFullName(format.field);
                for (String fieldName: fieldNames) {
                   docValueFields.add(new DocValueFieldsContext.FieldAndFormat(fieldName, format.format));
                }
            }
            int maxAllowedDocvalueFields = context.mapperService().getIndexSettings().getMaxDocvalueFields();
            if (docValueFields.size() > maxAllowedDocvalueFields) {
                throw new IllegalArgumentException(
                    "Trying to retrieve too many docvalue_fields. Must be less than or equal to: [" + maxAllowedDocvalueFields
                        + "] but was [" + docValueFields.size() + "]. This limit can be set by changing the ["
                        + IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.getKey() + "] index level setting.");
            }
            context.docValueFieldsContext(new DocValueFieldsContext(docValueFields));
        }
        if (source.highlighter() != null) {
            HighlightBuilder highlightBuilder = source.highlighter();
            try {
                context.highlight(highlightBuilder.build(queryShardContext));
            } catch (IOException e) {
                throw new SearchException(shardTarget, "failed to create SearchContextHighlighter", e);
            }
        }
        if (source.scriptFields() != null && source.size() != 0) {
            int maxAllowedScriptFields = context.mapperService().getIndexSettings().getMaxScriptFields();
            if (source.scriptFields().size() > maxAllowedScriptFields) {
                throw new IllegalArgumentException(
                        "Trying to retrieve too many script_fields. Must be less than or equal to: [" + maxAllowedScriptFields
                                + "] but was [" + source.scriptFields().size() + "]. This limit can be set by changing the ["
                                + IndexSettings.MAX_SCRIPT_FIELDS_SETTING.getKey() + "] index level setting.");
            }
            for (org.elasticsearch.search.builder.SearchSourceBuilder.ScriptField field : source.scriptFields()) {
                FieldScript.Factory factory = scriptService.compile(field.script(), FieldScript.CONTEXT);
                FieldScript.LeafFactory searchScript = factory.newFactory(field.script().getParams(), context.lookup());
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

        if (source.seqNoAndPrimaryTerm() != null) {
            context.seqNoAndPrimaryTerm(source.seqNoAndPrimaryTerm());
        }

        if (source.stats() != null) {
            context.groupStats(source.stats());
        }
        if (source.searchAfter() != null && source.searchAfter().length > 0) {
            if (context.scrollContext() != null) {
                throw new SearchException(shardTarget, "`search_after` cannot be used in a scroll context.");
            }
            if (context.from() > 0) {
                throw new SearchException(shardTarget, "`from` parameter must be set to 0 when `search_after` is used.");
            }
            FieldDoc fieldDoc = SearchAfterBuilder.buildFieldDoc(context.sort(), source.searchAfter());
            context.searchAfter(fieldDoc);
        }

        if (source.slice() != null) {
            if (context.scrollContext() == null) {
                throw new SearchException(shardTarget, "`slice` cannot be used outside of a scroll context");
            }
            context.sliceBuilder(source.slice());
        }

        if (source.storedFields() != null) {
            if (source.storedFields().fetchFields() == false) {
                if (context.version()) {
                    throw new SearchException(shardTarget, "`stored_fields` cannot be disabled if version is requested");
                }
                if (context.sourceRequested()) {
                    throw new SearchException(shardTarget, "`stored_fields` cannot be disabled if _source is requested");
                }
            }
            context.storedFieldsContext(source.storedFields());
        }

        if (source.collapse() != null) {
            if (context.scrollContext() != null) {
                throw new SearchException(shardTarget, "cannot use `collapse` in a scroll context");
            }
            if (context.searchAfter() != null) {
                throw new SearchException(shardTarget, "cannot use `collapse` in conjunction with `search_after`");
            }
            if (context.rescore() != null && context.rescore().isEmpty() == false) {
                throw new SearchException(shardTarget, "cannot use `collapse` in conjunction with `rescore`");
            }
            final CollapseContext collapseContext = source.collapse().build(queryShardContext);
            context.collapse(collapseContext);
        }
    }

    /**
     * Shortcut ids to load, we load only "from" and up to "size". The phase controller
     * handles this as well since the result is always size * shards for Q_T_F
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
            TopDocs topDocs = context.queryResult().topDocs().topDocs;
            docIdsToLoad = new int[topDocs.scoreDocs.length + numSuggestDocs];
            for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                docIdsToLoad[docsOffset++] = topDocs.scoreDocs[i].doc;
            }
        } else {
            TopDocs topDocs = context.queryResult().topDocs().topDocs;
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

    private void processScroll(InternalScrollSearchRequest request, SearchContext context) {
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

    public AliasFilter buildAliasFilter(ClusterState state, String index, Set<String> resolvedExpressions) {
        return indicesService.buildAliasFilter(state, index, resolvedExpressions);
    }

    /**
     * This method does a very quick rewrite of the query and returns true if the query can potentially match any documents.
     * This method can have false positives while if it returns <code>false</code> the query won't match any documents on the current
     * shard.
     */
    public CanMatchResponse canMatch(ShardSearchRequest request) throws IOException {
        assert request.searchType() == SearchType.QUERY_THEN_FETCH : "unexpected search type: " + request.searchType();
        IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(request.shardId().getId());
        // we don't want to use the reader wrapper since it could run costly operations
        // and we can afford false positives.
        try (Engine.Searcher searcher = indexShard.acquireSearcherNoWrap("can_match")) {
            QueryShardContext context = indexService.newQueryShardContext(request.shardId().id(), searcher,
                request::nowInMillis, request.getClusterAlias());
            Rewriteable.rewrite(request.getRewriteable(), context, false);
            FieldSortBuilder sortBuilder = FieldSortBuilder.getPrimaryFieldSortOrNull(request.source());
            MinAndMax<?> minMax = sortBuilder != null ? FieldSortBuilder.getMinMaxOrNull(context, sortBuilder) : null;
            if (canRewriteToMatchNone(request.source())) {
                QueryBuilder queryBuilder = request.source().query();
                return new CanMatchResponse(queryBuilder instanceof MatchNoneQueryBuilder == false, minMax);
            }
            // null query means match_all
            return new CanMatchResponse(true, minMax);
        }
    }

    public void canMatch(ShardSearchRequest request, ActionListener<CanMatchResponse> listener) {
        try {
            listener.onResponse(canMatch(request));
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    /**
     * Returns true iff the given search source builder can be early terminated by rewriting to a match none query. Or in other words
     * if the execution of the search request can be early terminated without executing it. This is for instance not possible if
     * a global aggregation is part of this request or if there is a suggest builder present.
     */
    public static boolean canRewriteToMatchNone(SearchSourceBuilder source) {
        if (source == null || source.query() == null || source.query() instanceof MatchAllQueryBuilder || source.suggest() != null) {
            return false;
        }
        AggregatorFactories.Builder aggregations = source.aggregations();
        return aggregations == null || aggregations.mustVisitAllDocs() == false;
    }

    /*
     * Rewrites the search request with a light weight rewrite context in order to fetch resources asynchronously
     * and then rewrites with a searcher when the shard is active.
     * The provided action listener is executed on the same thread or in a listener threadpool.
     */
    private void rewriteShardRequest(ShardSearchRequest request, ActionListener<SearchRewriteContext> listener) {
        IndexShard shard = indicesService.indexServiceSafe(request.shardId().getIndex()).getShard(request.shardId().id());
        ActionListener<Rewriteable> actionListener = ActionListener.wrap(r ->
                // now we need to check if there is a pending refresh and register
                shard.awaitShardSearchActive(b -> {
                    try {
                        // we can now acquire a searcher and rewrite the request with it
                        SearchRewriteContext rewriteContext = acquireSearcherAndRewrite(request, shard);
                        listener.onResponse(rewriteContext);
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }), listener::onFailure);
        // we also do rewrite on the coordinating node (TransportSearchService) but we also need to do it here for BWC as well as
        // AliasFilters that might need to be rewritten. These are edge-cases but we are every efficient doing the rewrite here so it's not
        // adding a lot of overhead
        Rewriteable.rewriteAndFetch(request.getRewriteable(), indicesService.getRewriteContext(request::nowInMillis), actionListener);
    }

    SearchRewriteContext acquireSearcherAndRewrite(ShardSearchRequest request, IndexShard shard) throws IOException {
        // acquire the searcher for rewrite with no wrapping in order to avoid costly
        // operations. We'll wrap the searcher at a later stage (when executing the query).
        Engine.Searcher searcher = shard.acquireSearcherNoWrap("search");
        boolean success = false;
        try {
            IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
            QueryShardContext context = indexService.newQueryShardContext(request.shardId().id(), searcher,
                request::nowInMillis, request.getClusterAlias());
            Rewriteable.rewrite(request.getRewriteable(), context, true);
            SearchRewriteContext rewrite = new SearchRewriteContext(request, shard, searcher, getExecutor(shard));
            success = true;
            return rewrite;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(searcher);
            }
        }
    }

    /**
     * Returns a new {@link QueryRewriteContext} with the given {@code now} provider
     */
    public QueryRewriteContext getRewriteContext(LongSupplier nowInMillis) {
        return indicesService.getRewriteContext(nowInMillis);
    }

    public IndicesService getIndicesService() {
        return indicesService;
    }

    public InternalAggregation.ReduceContext createReduceContext(boolean finalReduce) {
        return new InternalAggregation.ReduceContext(bigArrays, scriptService,
            finalReduce ? multiBucketConsumerService.create() : bucketCount -> {}, finalReduce);
    }

    static class SearchRewriteContext {
        private final ShardSearchRequest request;
        private final IndexShard shard;
        private Engine.Searcher searcher;
        private final Executor executor;

        private boolean isWrapped;

        private SearchRewriteContext(ShardSearchRequest request,
                                     IndexShard shard,
                                     Engine.Searcher searcher,
                                     Executor executor) {
            this.request = request;
            this.shard = shard;
            this.searcher = searcher;
            this.executor = executor;
        }

        SearchRewriteContext wrapSearcher() {
            assert isWrapped == false : "searcher already wrapped";
            isWrapped = true;
            searcher = shard.wrapSearcher(searcher);
            return this;
        }

        void execute(Runnable runnable) {
            assert isWrapped : "searcher is not wrapped";
            executor.execute(runnable);
        }
    }

    public static final class CanMatchResponse extends SearchPhaseResult {
        private final boolean canMatch;
        private final MinAndMax<?> minAndMax;

        public CanMatchResponse(StreamInput in) throws IOException {
            super(in);
            this.canMatch = in.readBoolean();
            if (in.getVersion().onOrAfter(Version.V_7_6_0)) {
                minAndMax = in.readOptionalWriteable(MinAndMax::new);
            } else {
                minAndMax = null;
            }
        }

        public CanMatchResponse(boolean canMatch, MinAndMax<?> minAndMax) {
            this.canMatch = canMatch;
            this.minAndMax = minAndMax;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(canMatch);
            if (out.getVersion().onOrAfter(Version.V_7_6_0)) {
                out.writeOptionalWriteable(minAndMax);
            }
        }

        public boolean canMatch() {
            return canMatch;
        }

        public MinAndMax<?> minAndMax() {
            return minAndMax;
        }
    }

    /**
     * This helper class ensures we only execute either the success or the failure path for {@link SearchOperationListener}.
     * This is crucial for some implementations like {@link org.elasticsearch.index.search.stats.ShardSearchStats}.
     */
    private static final class SearchOperationListenerExecutor implements AutoCloseable {
        private final SearchOperationListener listener;
        private final SearchContext context;
        private final long time;
        private final boolean fetch;
        private long afterQueryTime = -1;
        private boolean closed = false;

        SearchOperationListenerExecutor(SearchContext context) {
            this(context, false, System.nanoTime());
        }

        SearchOperationListenerExecutor(SearchContext context, boolean fetch, long startTime) {
            this.listener = context.indexShard().getSearchOperationListener();
            this.context = context;
            time = startTime;
            this.fetch = fetch;
            if (fetch) {
                listener.onPreFetchPhase(context);
            } else {
                listener.onPreQueryPhase(context);
            }
        }

        long success() {
            return afterQueryTime = System.nanoTime();
        }

        @Override
        public void close() {
            assert closed == false : "already closed - while technically ok double closing is a likely a bug in this case";
            if (closed == false) {
                closed = true;
                if (afterQueryTime != -1) {
                    if (fetch) {
                        listener.onFetchPhase(context, afterQueryTime - time);
                    } else {
                        listener.onQueryPhase(context, afterQueryTime - time);
                    }
                } else {
                    if (fetch) {
                        listener.onFailedFetchPhase(context);
                    } else {
                        listener.onFailedQueryPhase(context);
                    }
                }
            }
        }
    }
}
