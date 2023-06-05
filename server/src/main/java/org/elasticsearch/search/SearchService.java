
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.search.CanMatchNodeRequest;
import org.elasticsearch.action.search.CanMatchNodeResponse;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.CoordinatorRewriteContextProvider;
import org.elasticsearch.index.query.InnerHitContextBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.GlobalCheckpointListeners;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.elasticsearch.node.ResponseCollectorService;
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.AggregationContext.ProductionAggregationContext;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseContext;
import org.elasticsearch.search.dfs.DfsPhase;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ScrollQueryFetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchRequest;
import org.elasticsearch.search.fetch.subphase.FetchDocValuesContext;
import org.elasticsearch.search.fetch.subphase.FetchFieldsContext;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsContext.ScriptField;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.LegacyReaderContext;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.internal.SubSearchContext;
import org.elasticsearch.search.lookup.SearchLookup;
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
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.core.TimeValue.timeValueHours;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueMinutes;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

public class SearchService extends AbstractLifecycleComponent implements IndexEventListener {
    private static final Logger logger = LogManager.getLogger(SearchService.class);

    // we can have 5 minutes here, since we make sure to clean with search requests and when shard/index closes
    public static final Setting<TimeValue> DEFAULT_KEEPALIVE_SETTING = Setting.positiveTimeSetting(
        "search.default_keep_alive",
        timeValueMinutes(5),
        Property.NodeScope,
        Property.Dynamic
    );
    public static final Setting<TimeValue> MAX_KEEPALIVE_SETTING = Setting.positiveTimeSetting(
        "search.max_keep_alive",
        timeValueHours(24),
        Property.NodeScope,
        Property.Dynamic
    );
    public static final Setting<TimeValue> KEEPALIVE_INTERVAL_SETTING = Setting.positiveTimeSetting(
        "search.keep_alive_interval",
        timeValueMinutes(1),
        Property.NodeScope
    );
    public static final Setting<Boolean> ALLOW_EXPENSIVE_QUERIES = Setting.boolSetting(
        "search.allow_expensive_queries",
        true,
        Property.NodeScope,
        Property.Dynamic
    );

    public static final Setting<Boolean> CCS_VERSION_CHECK_SETTING = Setting.boolSetting(
        "search.check_ccs_compatibility",
        false,
        Property.NodeScope
    );

    /**
     * Enables low-level, frequent search cancellation checks. Enabling low-level checks will make long running searches to react
     * to the cancellation request faster. It will produce more cancellation checks but benchmarking has shown these did not
     * noticeably slow down searches.
     */
    public static final Setting<Boolean> LOW_LEVEL_CANCELLATION_SETTING = Setting.boolSetting(
        "search.low_level_cancellation",
        true,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final TimeValue NO_TIMEOUT = timeValueMillis(-1);
    public static final Setting<TimeValue> DEFAULT_SEARCH_TIMEOUT_SETTING = Setting.timeSetting(
        "search.default_search_timeout",
        NO_TIMEOUT,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Boolean> DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS = Setting.boolSetting(
        "search.default_allow_partial_results",
        true,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Integer> MAX_OPEN_SCROLL_CONTEXT = Setting.intSetting(
        "search.max_open_scroll_context",
        500,
        0,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Boolean> ENABLE_REWRITE_AGGS_TO_FILTER_BY_FILTER = Setting.boolSetting(
        "search.aggs.rewrite_to_filter_by_filter",
        true,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<ByteSizeValue> MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING = Setting.byteSizeSetting(
        "search.max_async_search_response_size",
        new ByteSizeValue(10, ByteSizeUnit.MB),
        Property.Dynamic,
        Property.NodeScope
    );

    public static final int DEFAULT_SIZE = 10;
    public static final int DEFAULT_FROM = 0;

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final ScriptService scriptService;

    private final ResponseCollectorService responseCollectorService;

    private final ExecutorSelector executorSelector;

    private final BigArrays bigArrays;

    private final DfsPhase dfsPhase = new DfsPhase();

    private final FetchPhase fetchPhase;

    private volatile long defaultKeepAlive;

    private volatile long maxKeepAlive;

    private volatile TimeValue defaultSearchTimeout;

    private volatile boolean defaultAllowPartialSearchResults;

    private volatile boolean lowLevelCancellation;

    private volatile int maxOpenScrollContext;

    private volatile boolean enableRewriteAggsToFilterByFilter;

    private final Cancellable keepAliveReaper;

    private final AtomicLong idGenerator = new AtomicLong();

    private final Map<Long, ReaderContext> activeReaders = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    private final MultiBucketConsumerService multiBucketConsumerService;

    private final AtomicInteger openScrollContexts = new AtomicInteger();
    private final String sessionId = UUIDs.randomBase64UUID();

    private final Tracer tracer;

    public SearchService(
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ScriptService scriptService,
        BigArrays bigArrays,
        FetchPhase fetchPhase,
        ResponseCollectorService responseCollectorService,
        CircuitBreakerService circuitBreakerService,
        ExecutorSelector executorSelector,
        Tracer tracer
    ) {
        Settings settings = clusterService.getSettings();
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.scriptService = scriptService;
        this.responseCollectorService = responseCollectorService;
        this.bigArrays = bigArrays;
        this.fetchPhase = fetchPhase;
        this.multiBucketConsumerService = new MultiBucketConsumerService(
            clusterService,
            settings,
            circuitBreakerService.getBreaker(CircuitBreaker.REQUEST)
        );
        this.executorSelector = executorSelector;
        this.tracer = tracer;

        TimeValue keepAliveInterval = KEEPALIVE_INTERVAL_SETTING.get(settings);
        setKeepAlives(DEFAULT_KEEPALIVE_SETTING.get(settings), MAX_KEEPALIVE_SETTING.get(settings));

        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                DEFAULT_KEEPALIVE_SETTING,
                MAX_KEEPALIVE_SETTING,
                this::setKeepAlives,
                SearchService::validateKeepAlives
            );

        this.keepAliveReaper = threadPool.scheduleWithFixedDelay(new Reaper(), keepAliveInterval, Names.SAME);

        defaultSearchTimeout = DEFAULT_SEARCH_TIMEOUT_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DEFAULT_SEARCH_TIMEOUT_SETTING, this::setDefaultSearchTimeout);

        defaultAllowPartialSearchResults = DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.get(settings);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS, this::setDefaultAllowPartialSearchResults);

        maxOpenScrollContext = MAX_OPEN_SCROLL_CONTEXT.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_OPEN_SCROLL_CONTEXT, this::setMaxOpenScrollContext);

        lowLevelCancellation = LOW_LEVEL_CANCELLATION_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(LOW_LEVEL_CANCELLATION_SETTING, this::setLowLevelCancellation);

        enableRewriteAggsToFilterByFilter = ENABLE_REWRITE_AGGS_TO_FILTER_BY_FILTER.get(settings);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(ENABLE_REWRITE_AGGS_TO_FILTER_BY_FILTER, this::setEnableRewriteAggsToFilterByFilter);
    }

    private static void validateKeepAlives(TimeValue defaultKeepAlive, TimeValue maxKeepAlive) {
        if (defaultKeepAlive.millis() > maxKeepAlive.millis()) {
            throw new IllegalArgumentException(
                "Default keep alive setting for request ["
                    + DEFAULT_KEEPALIVE_SETTING.getKey()
                    + "]"
                    + " should be smaller than max keep alive ["
                    + MAX_KEEPALIVE_SETTING.getKey()
                    + "], "
                    + "was ("
                    + defaultKeepAlive
                    + " > "
                    + maxKeepAlive
                    + ")"
            );
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

    private void setEnableRewriteAggsToFilterByFilter(boolean enableRewriteAggsToFilterByFilter) {
        this.enableRewriteAggsToFilterByFilter = enableRewriteAggsToFilterByFilter;
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

    @Override
    public void beforeIndexShardCreated(ShardRouting routing, Settings indexSettings) {
        // if a shard is reassigned to a node where we still have searches against the same shard and it is not a relocate, we prefer
        // to stop searches to restore full availability as fast as possible. A known scenario here is that we lost connection to master
        // or master(s) were restarted.
        assert routing.initializing();
        if (routing.isRelocationTarget() == false) {
            freeAllContextsForShard(routing.shardId());
        }
    }

    protected void putReaderContext(ReaderContext context) {
        final ReaderContext previous = activeReaders.put(context.id().getId(), context);
        assert previous == null;
        // ensure that if we race against afterIndexRemoved, we remove the context from the active list.
        // this is important to ensure store can be cleaned up, in particular if the search is a scroll with a long timeout.
        final Index index = context.indexShard().shardId().getIndex();
        if (indicesService.hasIndex(index) == false) {
            removeReaderContext(context.id().getId());
            throw new IndexNotFoundException(index);
        }
    }

    protected ReaderContext removeReaderContext(long id) {
        return activeReaders.remove(id);
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {
        for (final ReaderContext context : activeReaders.values()) {
            freeReaderContext(context.id());
        }
    }

    @Override
    protected void doClose() {
        doStop();
        keepAliveReaper.cancel();
    }

    public void executeDfsPhase(ShardSearchRequest request, SearchShardTask task, ActionListener<SearchPhaseResult> listener) {
        final IndexShard shard = getShard(request);
        rewriteAndFetchShardRequest(shard, request, listener.delegateFailure((l, rewritten) -> {
            // fork the execution in the search thread pool
            ensureAfterSeqNoRefreshed(shard, request, () -> executeDfsPhase(request, task), l);
        }));
    }

    private DfsSearchResult executeDfsPhase(ShardSearchRequest request, SearchShardTask task) throws IOException {
        ReaderContext readerContext = createOrGetReaderContext(request);
        try (
            Releasable scope = tracer.withScope(task);
            Releasable ignored = readerContext.markAsUsed(getKeepAlive(request));
            SearchContext context = createContext(readerContext, request, task, ResultsType.DFS, false)
        ) {
            dfsPhase.execute(context);
            return context.dfsResult();
        } catch (Exception e) {
            logger.trace("Dfs phase failed", e);
            processFailure(readerContext, e);
            throw e;
        }
    }

    /**
     * Try to load the query results from the cache or execute the query phase directly if the cache cannot be used.
     */
    private void loadOrExecuteQueryPhase(final ShardSearchRequest request, final SearchContext context) throws Exception {
        final boolean canCache = IndicesService.canCache(request, context);
        context.getSearchExecutionContext().freezeContext();
        if (canCache) {
            indicesService.loadIntoContext(request, context);
        } else {
            QueryPhase.execute(context);
        }
    }

    public void executeQueryPhase(ShardSearchRequest request, SearchShardTask task, ActionListener<SearchPhaseResult> listener) {
        assert request.canReturnNullResponseIfMatchNoDocs() == false || request.numberOfShards() > 1
            : "empty responses require more than one shard";
        final IndexShard shard = getShard(request);
        rewriteAndFetchShardRequest(shard, request, listener.delegateFailure((l, orig) -> {
            // check if we can shortcut the query phase entirely.
            if (orig.canReturnNullResponseIfMatchNoDocs()) {
                assert orig.scroll() == null;
                final CanMatchShardResponse canMatchResp;
                try {
                    ShardSearchRequest clone = new ShardSearchRequest(orig);
                    canMatchResp = canMatch(clone, false);
                } catch (Exception exc) {
                    l.onFailure(exc);
                    return;
                }
                if (canMatchResp.canMatch() == false) {
                    l.onResponse(QuerySearchResult.nullInstance());
                    return;
                }
            }
            ensureAfterSeqNoRefreshed(shard, orig, () -> executeQueryPhase(orig, task), l);
        }));
    }

    private <T> void ensureAfterSeqNoRefreshed(
        IndexShard shard,
        ShardSearchRequest request,
        CheckedSupplier<T, Exception> executable,
        ActionListener<T> listener
    ) {
        final long waitForCheckpoint = request.waitForCheckpoint();
        final Executor executor = getExecutor(shard);
        try {
            if (waitForCheckpoint <= UNASSIGNED_SEQ_NO) {
                runAsync(executor, executable, listener);
                return;
            }
            if (shard.indexSettings().getRefreshInterval().getMillis() <= 0) {
                listener.onFailure(new IllegalArgumentException("Cannot use wait_for_checkpoints with [index.refresh_interval=-1]"));
                return;
            }

            final AtomicBoolean isDone = new AtomicBoolean(false);
            // TODO: this logic should be improved, the timeout should be handled in a way that removes the listener from the logic in the
            // index shard on timeout so that a timed-out listener does not use up any listener slots.
            final TimeValue timeout = request.getWaitForCheckpointsTimeout();
            final Scheduler.ScheduledCancellable timeoutTask = NO_TIMEOUT.equals(timeout) ? null : threadPool.schedule(() -> {
                if (isDone.compareAndSet(false, true)) {
                    listener.onFailure(
                        new ElasticsearchTimeoutException("Wait for seq_no [{}] refreshed timed out [{}]", waitForCheckpoint, timeout)
                    );
                }
            }, timeout, Names.SAME);

            // allow waiting for not-yet-issued sequence number if shard isn't promotable to primary and the timeout is less than or equal
            // to 30s
            final boolean allowWaitForNotYetIssued = shard.routingEntry().isPromotableToPrimary() == false
                && NO_TIMEOUT.equals(timeout) == false
                && timeout.getSeconds() <= 30L;
            shard.addRefreshListener(waitForCheckpoint, allowWaitForNotYetIssued, new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    // We must check that the sequence number is smaller than or equal to the global checkpoint. If it is not,
                    // it is possible that a stale shard could return uncommitted documents.
                    if (shard.getLastKnownGlobalCheckpoint() >= waitForCheckpoint) {
                        searchReady();
                        return;
                    }
                    shard.addGlobalCheckpointListener(waitForCheckpoint, new GlobalCheckpointListeners.GlobalCheckpointListener() {
                        @Override
                        public Executor executor() {
                            return threadPool.executor(Names.SAME);
                        }

                        @Override
                        public void accept(long g, Exception e) {
                            if (g != UNASSIGNED_SEQ_NO) {
                                assert waitForCheckpoint <= g
                                    : shard.shardId() + " only advanced to [" + g + "] while waiting for [" + waitForCheckpoint + "]";
                                searchReady();
                            } else {
                                assert e != null;
                                // Ignore TimeoutException, our scheduled timeout task will handle this
                                if (e instanceof TimeoutException == false) {
                                    onFailure(e);
                                }
                            }
                        }
                    }, NO_TIMEOUT.equals(timeout) == false ? null : timeout);
                }

                @Override
                public void onFailure(Exception e) {
                    if (isDone.compareAndSet(false, true)) {
                        if (timeoutTask != null) {
                            timeoutTask.cancel();
                        }
                        listener.onFailure(e);
                    }
                }

                private void searchReady() {
                    if (isDone.compareAndSet(false, true)) {
                        if (timeoutTask != null) {
                            timeoutTask.cancel();
                        }
                        runAsync(executor, executable, listener);
                    }
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private IndexShard getShard(ShardSearchRequest request) {
        final ShardSearchContextId contextId = request.readerId();
        if (contextId != null) {
            if (sessionId.equals(contextId.getSessionId())) {
                final ReaderContext readerContext = activeReaders.get(contextId.getId());
                if (readerContext != null) {
                    return readerContext.indexShard();
                }
            }
        }
        return indicesService.indexServiceSafe(request.shardId().getIndex()).getShard(request.shardId().id());
    }

    private static <T> void runAsync(Executor executor, CheckedSupplier<T, Exception> executable, ActionListener<T> listener) {
        executor.execute(ActionRunnable.supply(listener, executable));
    }

    /**
     * The returned {@link SearchPhaseResult} will have had its ref count incremented by this method.
     * It is the responsibility of the caller to ensure that the ref count is correctly decremented
     * when the object is no longer needed.
     */
    private SearchPhaseResult executeQueryPhase(ShardSearchRequest request, SearchShardTask task) throws Exception {
        final ReaderContext readerContext = createOrGetReaderContext(request);
        try (
            Releasable scope = tracer.withScope(task);
            Releasable ignored = readerContext.markAsUsed(getKeepAlive(request));
            SearchContext context = createContext(readerContext, request, task, ResultsType.QUERY, true)
        ) {
            tracer.startTrace("executeQueryPhase", Map.of());
            final long afterQueryTime;
            try (SearchOperationListenerExecutor executor = new SearchOperationListenerExecutor(context)) {
                loadOrExecuteQueryPhase(request, context);
                if (context.queryResult().hasSearchContext() == false && readerContext.singleSession()) {
                    freeReaderContext(readerContext.id());
                }
                afterQueryTime = executor.success();
            } finally {
                tracer.stopTrace();
            }
            if (request.numberOfShards() == 1) {
                // we already have query results, but we can run fetch at the same time
                context.addFetchResult();
                return executeFetchPhase(readerContext, context, afterQueryTime);
            } else {
                // Pass the rescoreDocIds to the queryResult to send them the coordinating node and receive them back in the fetch phase.
                // We also pass the rescoreDocIds to the LegacyReaderContext in case the search state needs to stay in the data node.
                final RescoreDocIds rescoreDocIds = context.rescoreDocIds();
                context.queryResult().setRescoreDocIds(rescoreDocIds);
                readerContext.setRescoreDocIds(rescoreDocIds);
                context.queryResult().incRef();
                return context.queryResult();
            }
        } catch (Exception e) {
            // execution exception can happen while loading the cache, strip it
            if (e instanceof ExecutionException) {
                e = (e.getCause() == null || e.getCause() instanceof Exception)
                    ? (Exception) e.getCause()
                    : new ElasticsearchException(e.getCause());
            }
            logger.trace("Query phase failed", e);
            processFailure(readerContext, e);
            throw e;
        }
    }

    private QueryFetchSearchResult executeFetchPhase(ReaderContext reader, SearchContext context, long afterQueryTime) {
        try (
            Releasable scope = tracer.withScope(context.getTask());
            SearchOperationListenerExecutor executor = new SearchOperationListenerExecutor(context, true, afterQueryTime)
        ) {
            shortcutDocIdsToLoad(context);
            fetchPhase.execute(context);
            if (reader.singleSession()) {
                freeReaderContext(reader.id());
            }
            executor.success();
        }
        // This will incRef the QuerySearchResult when it gets created
        return new QueryFetchSearchResult(context.queryResult(), context.fetchResult());
    }

    public void executeQueryPhase(
        InternalScrollSearchRequest request,
        SearchShardTask task,
        ActionListener<ScrollQuerySearchResult> listener
    ) {
        final LegacyReaderContext readerContext = (LegacyReaderContext) findReaderContext(request.contextId(), request);
        final Releasable markAsUsed;
        try {
            markAsUsed = readerContext.markAsUsed(getScrollKeepAlive(request.scroll()));
        } catch (Exception e) {
            // We need to release the reader context of the scroll when we hit any exception (here the keep_alive can be too large)
            freeReaderContext(readerContext.id());
            throw e;
        }
        runAsync(getExecutor(readerContext.indexShard()), () -> {
            final ShardSearchRequest shardSearchRequest = readerContext.getShardSearchRequest(null);
            try (
                SearchContext searchContext = createContext(readerContext, shardSearchRequest, task, ResultsType.QUERY, false);
                SearchOperationListenerExecutor executor = new SearchOperationListenerExecutor(searchContext)
            ) {
                searchContext.searcher().setAggregatedDfs(readerContext.getAggregatedDfs(null));
                processScroll(request, readerContext, searchContext);
                QueryPhase.execute(searchContext);
                executor.success();
                readerContext.setRescoreDocIds(searchContext.rescoreDocIds());
                // ScrollQuerySearchResult will incRef the QuerySearchResult when it gets constructed.
                return new ScrollQuerySearchResult(searchContext.queryResult(), searchContext.shardTarget());
            } catch (Exception e) {
                logger.trace("Query phase failed", e);
                // we handle the failure in the failure listener below
                throw e;
            }
        }, wrapFailureListener(listener, readerContext, markAsUsed));
    }

    /**
     * The returned {@link SearchPhaseResult} will have had its ref count incremented by this method.
     * It is the responsibility of the caller to ensure that the ref count is correctly decremented
     * when the object is no longer needed.
     */
    public void executeQueryPhase(QuerySearchRequest request, SearchShardTask task, ActionListener<QuerySearchResult> listener) {
        final ReaderContext readerContext = findReaderContext(request.contextId(), request.shardSearchRequest());
        final ShardSearchRequest shardSearchRequest = readerContext.getShardSearchRequest(request.shardSearchRequest());
        final Releasable markAsUsed = readerContext.markAsUsed(getKeepAlive(shardSearchRequest));
        runAsync(getExecutor(readerContext.indexShard()), () -> {
            readerContext.setAggregatedDfs(request.dfs());
            try (
                SearchContext searchContext = createContext(readerContext, shardSearchRequest, task, ResultsType.QUERY, true);
                SearchOperationListenerExecutor executor = new SearchOperationListenerExecutor(searchContext)
            ) {
                searchContext.searcher().setAggregatedDfs(request.dfs());
                QueryPhase.execute(searchContext);
                if (searchContext.queryResult().hasSearchContext() == false && readerContext.singleSession()) {
                    // no hits, we can release the context since there will be no fetch phase
                    freeReaderContext(readerContext.id());
                }
                executor.success();
                // Pass the rescoreDocIds to the queryResult to send them the coordinating node and receive them back in the fetch phase.
                // We also pass the rescoreDocIds to the LegacyReaderContext in case the search state needs to stay in the data node.
                final RescoreDocIds rescoreDocIds = searchContext.rescoreDocIds();
                searchContext.queryResult().setRescoreDocIds(rescoreDocIds);
                readerContext.setRescoreDocIds(rescoreDocIds);
                searchContext.queryResult().incRef();
                return searchContext.queryResult();
            } catch (Exception e) {
                assert TransportActions.isShardNotAvailableException(e) == false : new AssertionError(e);
                logger.trace("Query phase failed", e);
                // we handle the failure in the failure listener below
                throw e;
            }
        }, wrapFailureListener(listener, readerContext, markAsUsed));
    }

    private Executor getExecutor(IndexShard indexShard) {
        assert indexShard != null;
        final String executorName;
        if (indexShard.isSystem()) {
            executorName = executorSelector.executorForSearch(indexShard.shardId().getIndexName());
        } else if (indexShard.indexSettings().isSearchThrottled()) {
            executorName = Names.SEARCH_THROTTLED;
        } else {
            executorName = Names.SEARCH;
        }
        return threadPool.executor(executorName);
    }

    public void executeFetchPhase(
        InternalScrollSearchRequest request,
        SearchShardTask task,
        ActionListener<ScrollQueryFetchSearchResult> listener
    ) {
        final LegacyReaderContext readerContext = (LegacyReaderContext) findReaderContext(request.contextId(), request);
        final Releasable markAsUsed;
        try {
            markAsUsed = readerContext.markAsUsed(getScrollKeepAlive(request.scroll()));
        } catch (Exception e) {
            // We need to release the reader context of the scroll when we hit any exception (here the keep_alive can be too large)
            freeReaderContext(readerContext.id());
            throw e;
        }
        runAsync(getExecutor(readerContext.indexShard()), () -> {
            final ShardSearchRequest shardSearchRequest = readerContext.getShardSearchRequest(null);
            try (
                SearchContext searchContext = createContext(readerContext, shardSearchRequest, task, ResultsType.FETCH, false);
                SearchOperationListenerExecutor executor = new SearchOperationListenerExecutor(searchContext)
            ) {
                searchContext.assignRescoreDocIds(readerContext.getRescoreDocIds(null));
                searchContext.searcher().setAggregatedDfs(readerContext.getAggregatedDfs(null));
                processScroll(request, readerContext, searchContext);
                searchContext.addQueryResult();
                QueryPhase.execute(searchContext);
                final long afterQueryTime = executor.success();
                QueryFetchSearchResult fetchSearchResult = executeFetchPhase(readerContext, searchContext, afterQueryTime);
                return new ScrollQueryFetchSearchResult(fetchSearchResult, searchContext.shardTarget());
            } catch (Exception e) {
                assert TransportActions.isShardNotAvailableException(e) == false : new AssertionError(e);
                logger.trace("Fetch phase failed", e);
                // we handle the failure in the failure listener below
                throw e;
            }
        }, wrapFailureListener(listener, readerContext, markAsUsed));
    }

    public void executeFetchPhase(ShardFetchRequest request, SearchShardTask task, ActionListener<FetchSearchResult> listener) {
        final ReaderContext readerContext = findReaderContext(request.contextId(), request);
        final ShardSearchRequest shardSearchRequest = readerContext.getShardSearchRequest(request.getShardSearchRequest());
        final Releasable markAsUsed = readerContext.markAsUsed(getKeepAlive(shardSearchRequest));
        runAsync(getExecutor(readerContext.indexShard()), () -> {
            try (SearchContext searchContext = createContext(readerContext, shardSearchRequest, task, ResultsType.FETCH, false)) {
                if (request.lastEmittedDoc() != null) {
                    searchContext.scrollContext().lastEmittedDoc = request.lastEmittedDoc();
                }
                searchContext.assignRescoreDocIds(readerContext.getRescoreDocIds(request.getRescoreDocIds()));
                searchContext.searcher().setAggregatedDfs(readerContext.getAggregatedDfs(request.getAggregatedDfs()));
                searchContext.docIdsToLoad(request.docIds());
                try (
                    SearchOperationListenerExecutor executor = new SearchOperationListenerExecutor(searchContext, true, System.nanoTime())
                ) {
                    fetchPhase.execute(searchContext);
                    if (readerContext.singleSession()) {
                        freeReaderContext(request.contextId());
                    }
                    executor.success();
                }
                return searchContext.fetchResult();
            } catch (Exception e) {
                assert TransportActions.isShardNotAvailableException(e) == false : new AssertionError(e);
                // we handle the failure in the failure listener below
                throw e;
            }
        }, wrapFailureListener(listener, readerContext, markAsUsed));
    }

    protected void checkCancelled(SearchShardTask task) {
        // check cancellation as early as possible, as it avoids opening up a Lucene reader on FrozenEngine
        try {
            task.ensureNotCancelled();
        } catch (TaskCancelledException e) {
            logger.trace("task cancelled [id: {}, action: {}]", task.getId(), task.getAction());
            throw e;
        }
    }

    private ReaderContext findReaderContext(ShardSearchContextId id, TransportRequest request) throws SearchContextMissingException {
        if (id.getSessionId().isEmpty()) {
            throw new IllegalArgumentException("Session id must be specified");
        }
        if (sessionId.equals(id.getSessionId()) == false) {
            throw new SearchContextMissingException(id);
        }
        final ReaderContext reader = activeReaders.get(id.getId());
        if (reader == null) {
            throw new SearchContextMissingException(id);
        }
        try {
            reader.validate(request);
        } catch (Exception exc) {
            processFailure(reader, exc);
            throw exc;
        }
        return reader;
    }

    final ReaderContext createOrGetReaderContext(ShardSearchRequest request) {
        if (request.readerId() != null) {
            try {
                return findReaderContext(request.readerId(), request);
            } catch (SearchContextMissingException e) {
                final String searcherId = request.readerId().getSearcherId();
                if (searcherId == null) {
                    throw e;
                }
                final IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
                final IndexShard shard = indexService.getShard(request.shardId().id());
                final Engine.SearcherSupplier searcherSupplier = shard.acquireSearcherSupplier();
                if (searcherId.equals(searcherSupplier.getSearcherId()) == false) {
                    searcherSupplier.close();
                    throw e;
                }
                return createAndPutReaderContext(request, indexService, shard, searcherSupplier, defaultKeepAlive);
            }
        } else {
            final long keepAliveInMillis = getKeepAlive(request);
            final IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
            final IndexShard shard = indexService.getShard(request.shardId().id());
            final Engine.SearcherSupplier searcherSupplier = shard.acquireSearcherSupplier();
            return createAndPutReaderContext(request, indexService, shard, searcherSupplier, keepAliveInMillis);
        }
    }

    final ReaderContext createAndPutReaderContext(
        ShardSearchRequest request,
        IndexService indexService,
        IndexShard shard,
        Engine.SearcherSupplier reader,
        long keepAliveInMillis
    ) {
        ReaderContext readerContext = null;
        Releasable decreaseScrollContexts = null;
        try {
            if (request.scroll() != null) {
                decreaseScrollContexts = openScrollContexts::decrementAndGet;
                if (openScrollContexts.incrementAndGet() > maxOpenScrollContext) {
                    throw new ElasticsearchException(
                        "Trying to create too many scroll contexts. Must be less than or equal to: ["
                            + maxOpenScrollContext
                            + "]. "
                            + "This limit can be set by changing the ["
                            + MAX_OPEN_SCROLL_CONTEXT.getKey()
                            + "] setting."
                    );
                }
            }
            final ShardSearchContextId id = new ShardSearchContextId(sessionId, idGenerator.incrementAndGet());
            if (request.scroll() != null) {
                readerContext = new LegacyReaderContext(id, indexService, shard, reader, request, keepAliveInMillis);
                if (request.scroll() != null) {
                    readerContext.addOnClose(decreaseScrollContexts);
                    decreaseScrollContexts = null;
                }
            } else {
                readerContext = new ReaderContext(id, indexService, shard, reader, keepAliveInMillis, true);
            }
            reader = null;
            final ReaderContext finalReaderContext = readerContext;
            final SearchOperationListener searchOperationListener = shard.getSearchOperationListener();
            searchOperationListener.onNewReaderContext(finalReaderContext);
            if (finalReaderContext.scrollContext() != null) {
                searchOperationListener.onNewScrollContext(finalReaderContext);
            }
            readerContext.addOnClose(() -> {
                try {
                    if (finalReaderContext.scrollContext() != null) {
                        searchOperationListener.onFreeScrollContext(finalReaderContext);
                    }
                } finally {
                    searchOperationListener.onFreeReaderContext(finalReaderContext);
                }
            });
            putReaderContext(finalReaderContext);
            readerContext = null;
            return finalReaderContext;
        } finally {
            Releasables.close(reader, readerContext, decreaseScrollContexts);
        }
    }

    /**
     * Opens the reader context for given shardId. The newly opened reader context will be keep
     * until the {@code keepAlive} elapsed unless it is manually released.
     */
    public void openReaderContext(ShardId shardId, TimeValue keepAlive, ActionListener<ShardSearchContextId> listener) {
        checkKeepAliveLimit(keepAlive.millis());
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final IndexShard shard = indexService.getShard(shardId.id());
        final SearchOperationListener searchOperationListener = shard.getSearchOperationListener();
        shard.awaitShardSearchActive(ignored -> {
            Engine.SearcherSupplier searcherSupplier = null;
            ReaderContext readerContext = null;
            try {
                searcherSupplier = shard.acquireSearcherSupplier();
                final ShardSearchContextId id = new ShardSearchContextId(
                    sessionId,
                    idGenerator.incrementAndGet(),
                    searcherSupplier.getSearcherId()
                );
                readerContext = new ReaderContext(id, indexService, shard, searcherSupplier, keepAlive.millis(), false);
                final ReaderContext finalReaderContext = readerContext;
                searcherSupplier = null; // transfer ownership to reader context
                searchOperationListener.onNewReaderContext(readerContext);
                readerContext.addOnClose(() -> searchOperationListener.onFreeReaderContext(finalReaderContext));
                putReaderContext(readerContext);
                readerContext = null;
                listener.onResponse(finalReaderContext.id());
            } catch (Exception exc) {
                Releasables.closeWhileHandlingException(searcherSupplier, readerContext);
                listener.onFailure(exc);
            }
        });
    }

    protected SearchContext createContext(
        ReaderContext readerContext,
        ShardSearchRequest request,
        SearchShardTask task,
        ResultsType resultsType,
        boolean includeAggregations
    ) throws IOException {
        checkCancelled(task);
        final DefaultSearchContext context = createSearchContext(readerContext, request, defaultSearchTimeout);
        resultsType.addResultsObject(context);
        try {
            if (request.scroll() != null) {
                context.scrollContext().scroll = request.scroll();
            }
            parseSource(context, request.source(), includeAggregations);

            // if the from and size are still not set, default them
            if (context.from() == -1) {
                context.from(DEFAULT_FROM);
            }
            if (context.size() == -1) {
                context.size(DEFAULT_SIZE);
            }
            if (request.rankQueryBuilders().isEmpty() == false) {
                List<Query> rankQueries = new ArrayList<>();
                for (QueryBuilder queryBuilder : request.rankQueryBuilders()) {
                    rankQueries.add(queryBuilder.toQuery(context.getSearchExecutionContext()));
                }
                context.rankShardContext(request.source().rankBuilder().buildRankShardContext(rankQueries, context.from()));
            }
            context.setTask(task);

            context.preProcess();
        } catch (Exception e) {
            context.close();
            throw e;
        }

        return context;
    }

    public DefaultSearchContext createSearchContext(ShardSearchRequest request, TimeValue timeout) throws IOException {
        final IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        final IndexShard indexShard = indexService.getShard(request.shardId().getId());
        final Engine.SearcherSupplier reader = indexShard.acquireSearcherSupplier();
        final ShardSearchContextId id = new ShardSearchContextId(sessionId, idGenerator.incrementAndGet());
        try (ReaderContext readerContext = new ReaderContext(id, indexService, indexShard, reader, -1L, true)) {
            DefaultSearchContext searchContext = createSearchContext(readerContext, request, timeout);
            searchContext.addReleasable(readerContext.markAsUsed(0L));
            return searchContext;
        }
    }

    @SuppressWarnings("unchecked")
    private DefaultSearchContext createSearchContext(ReaderContext reader, ShardSearchRequest request, TimeValue timeout)
        throws IOException {
        boolean success = false;
        DefaultSearchContext searchContext = null;
        try {
            SearchShardTarget shardTarget = new SearchShardTarget(
                clusterService.localNode().getId(),
                reader.indexShard().shardId(),
                request.getClusterAlias()
            );
            searchContext = new DefaultSearchContext(
                reader,
                request,
                shardTarget,
                threadPool::relativeTimeInMillis,
                timeout,
                fetchPhase,
                lowLevelCancellation
            );
            // we clone the query shard context here just for rewriting otherwise we
            // might end up with incorrect state since we are using now() or script services
            // during rewrite and normalized / evaluate templates etc.
            SearchExecutionContext context = new SearchExecutionContext(searchContext.getSearchExecutionContext());
            Rewriteable.rewrite(request.getRewriteable(), context, true);
            assert searchContext.getSearchExecutionContext().isCacheable();
            success = true;
        } finally {
            if (success == false) {
                // we handle the case where `IndicesService#indexServiceSafe`or `IndexService#getShard`, or the DefaultSearchContext
                // constructor throws an exception since we would otherwise leak a searcher and this can have severe implications
                // (unable to obtain shard lock exceptions).
                IOUtils.closeWhileHandlingException(searchContext);
            }
        }
        return searchContext;
    }

    private void freeAllContextForIndex(Index index) {
        assert index != null;
        for (ReaderContext ctx : activeReaders.values()) {
            if (index.equals(ctx.indexShard().shardId().getIndex())) {
                freeReaderContext(ctx.id());
            }
        }
    }

    private void freeAllContextsForShard(ShardId shardId) {
        assert shardId != null;
        for (ReaderContext ctx : activeReaders.values()) {
            if (shardId.equals(ctx.indexShard().shardId())) {
                freeReaderContext(ctx.id());
            }
        }
    }

    public boolean freeReaderContext(ShardSearchContextId contextId) {
        if (sessionId.equals(contextId.getSessionId())) {
            try (ReaderContext context = removeReaderContext(contextId.getId())) {
                return context != null;
            }
        }
        return false;
    }

    public void freeAllScrollContexts() {
        for (ReaderContext readerContext : activeReaders.values()) {
            if (readerContext.scrollContext() != null) {
                freeReaderContext(readerContext.id());
            }
        }
    }

    private long getKeepAlive(ShardSearchRequest request) {
        if (request.scroll() != null) {
            return getScrollKeepAlive(request.scroll());
        } else if (request.keepAlive() != null) {
            checkKeepAliveLimit(request.keepAlive().millis());
            return request.keepAlive().getMillis();
        } else {
            return request.readerId() == null ? defaultKeepAlive : -1;
        }
    }

    private long getScrollKeepAlive(Scroll scroll) {
        if (scroll != null && scroll.keepAlive() != null) {
            checkKeepAliveLimit(scroll.keepAlive().millis());
            return scroll.keepAlive().getMillis();
        }
        return defaultKeepAlive;
    }

    private void checkKeepAliveLimit(long keepAlive) {
        if (keepAlive > maxKeepAlive) {
            throw new IllegalArgumentException(
                "Keep alive for request ("
                    + TimeValue.timeValueMillis(keepAlive)
                    + ") is too large. "
                    + "It must be less than ("
                    + TimeValue.timeValueMillis(maxKeepAlive)
                    + "). "
                    + "This limit can be set by changing the ["
                    + MAX_KEEPALIVE_SETTING.getKey()
                    + "] cluster level setting."
            );
        }
    }

    private <T> ActionListener<T> wrapFailureListener(ActionListener<T> listener, ReaderContext context, Releasable releasable) {
        return new ActionListener<>() {
            @Override
            public void onResponse(T resp) {
                Releasables.close(releasable);
                listener.onResponse(resp);
            }

            @Override
            public void onFailure(Exception exc) {
                processFailure(context, exc);
                Releasables.close(releasable);
                listener.onFailure(exc);
            }
        };
    }

    private static boolean isScrollContext(ReaderContext context) {
        return context instanceof LegacyReaderContext && context.singleSession() == false;
    }

    private void processFailure(ReaderContext context, Exception exc) {
        if (context.singleSession() || isScrollContext(context)) {
            // we release the reader on failure if the request is a normal search or a scroll
            freeReaderContext(context.id());
        }
        try {
            if (Lucene.isCorruptionException(exc)) {
                context.indexShard().failShard("search execution corruption failure", exc);
            }
        } catch (Exception inner) {
            inner.addSuppressed(exc);
            logger.warn("failed to process shard failure to (potentially) send back shard failure on corruption", inner);
        }
    }

    private void parseSource(DefaultSearchContext context, SearchSourceBuilder source, boolean includeAggregations) {
        // nothing to parse...
        if (source == null) {
            return;
        }
        SearchShardTarget shardTarget = context.shardTarget();
        SearchExecutionContext searchExecutionContext = context.getSearchExecutionContext();
        context.from(source.from());
        context.size(source.size());
        Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        if (source.query() != null) {
            InnerHitContextBuilder.extractInnerHits(source.query(), innerHitBuilders);
            context.parsedQuery(searchExecutionContext.toQuery(source.query()));
        }
        if (source.postFilter() != null) {
            InnerHitContextBuilder.extractInnerHits(source.postFilter(), innerHitBuilders);
            context.parsedPostFilter(searchExecutionContext.toQuery(source.postFilter()));
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
                Optional<SortAndFormats> optionalSort = SortBuilder.buildSort(source.sorts(), context.getSearchExecutionContext());
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
        if (source.aggregations() != null && includeAggregations) {
            AggregationContext aggContext = new ProductionAggregationContext(
                indicesService.getAnalysis(),
                context.getSearchExecutionContext(),
                bigArrays,
                source.aggregations().bytesToPreallocate(),
                /*
                 * The query on the search context right now doesn't include
                 * the filter for nested documents or slicing so we have to
                 * delay reading it until the aggs ask for it.
                 */
                () -> context.rewrittenQuery() == null ? new MatchAllDocsQuery() : context.rewrittenQuery(),
                context.getProfilers() == null ? null : context.getProfilers().getAggregationProfiler(),
                multiBucketConsumerService.getLimit(),
                () -> new SubSearchContext(context).parsedQuery(context.parsedQuery()).fetchFieldsContext(context.fetchFieldsContext()),
                context.bitsetFilterCache(),
                context.indexShard().shardId().hashCode(),
                context::getRelativeTimeInMillis,
                context::isCancelled,
                context::buildFilteredQuery,
                enableRewriteAggsToFilterByFilter,
                source.aggregations().isInSortOrderExecutionRequired()
            );
            context.addQuerySearchResultReleasable(aggContext);
            try {
                AggregatorFactories factories = source.aggregations().build(aggContext, null);
                context.aggregations(new SearchContextAggregations(factories));
            } catch (IOException e) {
                throw new AggregationInitializationException("Failed to create aggregators", e);
            }
        }
        if (source.suggest() != null) {
            try {
                context.suggest(source.suggest().build(searchExecutionContext));
            } catch (IOException e) {
                throw new SearchException(shardTarget, "failed to create SuggestionSearchContext", e);
            }
        }
        if (source.rescores() != null) {
            try {
                for (RescorerBuilder<?> rescore : source.rescores()) {
                    context.addRescore(rescore.buildContext(searchExecutionContext));
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
            FetchDocValuesContext docValuesContext = new FetchDocValuesContext(
                context.getSearchExecutionContext(),
                source.docValueFields()
            );
            context.docValuesContext(docValuesContext);
        }
        if (source.fetchFields() != null) {
            FetchFieldsContext fetchFieldsContext = new FetchFieldsContext(source.fetchFields());
            context.fetchFieldsContext(fetchFieldsContext);
        }
        if (source.highlighter() != null) {
            HighlightBuilder highlightBuilder = source.highlighter();
            try {
                context.highlight(highlightBuilder.build(searchExecutionContext));
            } catch (IOException e) {
                throw new SearchException(shardTarget, "failed to create SearchContextHighlighter", e);
            }
        }
        if (source.scriptFields() != null && source.size() != 0) {
            int maxAllowedScriptFields = searchExecutionContext.getIndexSettings().getMaxScriptFields();
            if (source.scriptFields().size() > maxAllowedScriptFields) {
                throw new IllegalArgumentException(
                    "Trying to retrieve too many script_fields. Must be less than or equal to: ["
                        + maxAllowedScriptFields
                        + "] but was ["
                        + source.scriptFields().size()
                        + "]. This limit can be set by changing the ["
                        + IndexSettings.MAX_SCRIPT_FIELDS_SETTING.getKey()
                        + "] index level setting."
                );
            }
            for (org.elasticsearch.search.builder.SearchSourceBuilder.ScriptField field : source.scriptFields()) {
                FieldScript.Factory factory = scriptService.compile(field.script(), FieldScript.CONTEXT);
                SearchLookup lookup = context.getSearchExecutionContext().lookup();
                // TODO delay this construction until the FetchPhase is executed so that we can
                // use the more efficient lookup built there
                FieldScript.LeafFactory searchScript = factory.newFactory(field.script().getParams(), lookup);
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
        if (CollectionUtils.isEmpty(source.searchAfter()) == false) {
            if (context.scrollContext() != null) {
                throw new SearchException(shardTarget, "`search_after` cannot be used in a scroll context.");
            }
            if (context.from() > 0) {
                throw new SearchException(shardTarget, "`from` parameter must be set to 0 when `search_after` is used.");
            }

            String collapseField = source.collapse() != null ? source.collapse().getField() : null;
            FieldDoc fieldDoc = SearchAfterBuilder.buildFieldDoc(context.sort(), source.searchAfter(), collapseField);
            context.searchAfter(fieldDoc);
        }

        if (source.slice() != null) {
            if (source.pointInTimeBuilder() == null && context.scrollContext() == null) {
                throw new SearchException(shardTarget, "[slice] can only be used with [scroll] or [point-in-time] requests");
            }
            context.sliceBuilder(source.slice());
        }

        if (source.storedFields() != null) {
            if (source.storedFields().fetchFields() == false) {
                if (context.sourceRequested()) {
                    throw new SearchException(shardTarget, "[stored_fields] cannot be disabled if [_source] is requested");
                }
                if (context.fetchFieldsContext() != null) {
                    throw new SearchException(shardTarget, "[stored_fields] cannot be disabled when using the [fields] option");
                }
            }
            context.storedFieldsContext(source.storedFields());
        }

        if (source.collapse() != null) {
            if (context.scrollContext() != null) {
                throw new SearchException(shardTarget, "cannot use `collapse` in a scroll context");
            }
            if (context.rescore() != null && context.rescore().isEmpty() == false) {
                throw new SearchException(shardTarget, "cannot use `collapse` in conjunction with `rescore`");
            }
            final CollapseContext collapseContext = source.collapse().build(searchExecutionContext);
            context.collapse(collapseContext);
        }
    }

    /**
     * Shortcut ids to load, we load only "from" and up to "size". The phase controller
     * handles this as well since the result is always size * shards for Q_T_F
     */
    private static void shortcutDocIdsToLoad(SearchContext context) {
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
                docIdsToLoad = new int[Math.min(topDocs.scoreDocs.length - context.from(), context.size()) + numSuggestDocs];
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
        context.docIdsToLoad(docIdsToLoad);
    }

    private static void processScroll(InternalScrollSearchRequest request, ReaderContext reader, SearchContext context) {
        // process scroll
        context.from(context.from() + context.size());
        context.scrollContext().scroll = request.scroll();
    }

    /**
     * Returns the number of active contexts in this
     * SearchService
     */
    public int getActiveContexts() {
        return this.activeReaders.size();
    }

    /**
     * Returns the number of scroll contexts opened on the node
     */
    public int getOpenScrollContexts() {
        return openScrollContexts.get();
    }

    public ResponseCollectorService getResponseCollectorService() {
        return this.responseCollectorService;
    }

    /**
     * Used to indicate which result object should be instantiated when creating a search context
     */
    protected enum ResultsType {
        DFS {
            @Override
            void addResultsObject(SearchContext context) {
                context.addDfsResult();
            }
        },
        QUERY {
            @Override
            void addResultsObject(SearchContext context) {
                context.addQueryResult();
            }
        },
        FETCH {
            @Override
            void addResultsObject(SearchContext context) {
                context.addFetchResult();
            }
        },
        /**
         * None is intended for use in testing, when we might not progress all the way to generating results
         */
        NONE {
            @Override
            void addResultsObject(SearchContext context) {
                // this space intentionally left blank
            }
        };

        abstract void addResultsObject(SearchContext context);
    }

    class Reaper implements Runnable {
        @Override
        public void run() {
            for (ReaderContext context : activeReaders.values()) {
                if (context.isExpired()) {
                    logger.debug("freeing search context [{}]", context.id());
                    freeReaderContext(context.id());
                }
            }
        }
    }

    public AliasFilter buildAliasFilter(ClusterState state, String index, Set<String> resolvedExpressions) {
        return indicesService.buildAliasFilter(state, index, resolvedExpressions);
    }

    public void canMatch(ShardSearchRequest request, ActionListener<CanMatchShardResponse> listener) {
        try {
            listener.onResponse(canMatch(request));
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    public void canMatch(CanMatchNodeRequest request, ActionListener<CanMatchNodeResponse> listener) {
        final List<ShardSearchRequest> shardSearchRequests = request.createShardSearchRequests();
        final List<CanMatchNodeResponse.ResponseOrFailure> responses = new ArrayList<>(shardSearchRequests.size());
        for (ShardSearchRequest shardSearchRequest : shardSearchRequests) {
            CanMatchShardResponse canMatchShardResponse;
            try {
                canMatchShardResponse = canMatch(shardSearchRequest);
                responses.add(new CanMatchNodeResponse.ResponseOrFailure(canMatchShardResponse));
            } catch (Exception e) {
                responses.add(new CanMatchNodeResponse.ResponseOrFailure(e));
            }
        }
        listener.onResponse(new CanMatchNodeResponse(responses));
    }

    /**
     * This method uses a lightweight searcher without wrapping (i.e., not open a full reader on frozen indices) to rewrite the query
     * to check if the query can match any documents. This method can have false positives while if it returns {@code false} the query
     * won't match any documents on the current shard.
     */
    public CanMatchShardResponse canMatch(ShardSearchRequest request) throws IOException {
        return canMatch(request, true);
    }

    private CanMatchShardResponse canMatch(ShardSearchRequest request, boolean checkRefreshPending) throws IOException {
        assert request.searchType() == SearchType.QUERY_THEN_FETCH : "unexpected search type: " + request.searchType();
        Releasable releasable = null;
        try {
            IndexService indexService;
            final boolean hasRefreshPending;
            final Engine.Searcher canMatchSearcher;
            if (request.readerId() != null) {
                hasRefreshPending = false;
                ReaderContext readerContext;
                Engine.Searcher searcher;
                try {
                    readerContext = findReaderContext(request.readerId(), request);
                    releasable = readerContext.markAsUsed(getKeepAlive(request));
                    indexService = readerContext.indexService();
                    searcher = readerContext.acquireSearcher(Engine.CAN_MATCH_SEARCH_SOURCE);
                } catch (SearchContextMissingException e) {
                    final String searcherId = request.readerId().getSearcherId();
                    if (searcherId == null) {
                        throw e;
                    }
                    indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
                    IndexShard indexShard = indexService.getShard(request.shardId().getId());
                    final Engine.SearcherSupplier searcherSupplier = indexShard.acquireSearcherSupplier();
                    if (searcherId.equals(searcherSupplier.getSearcherId()) == false) {
                        searcherSupplier.close();
                        throw e;
                    }
                    releasable = searcherSupplier;
                    searcher = searcherSupplier.acquireSearcher(Engine.CAN_MATCH_SEARCH_SOURCE);
                }
                canMatchSearcher = searcher;
            } else {
                indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
                IndexShard indexShard = indexService.getShard(request.shardId().getId());
                boolean needsWaitForRefresh = request.waitForCheckpoint() != UNASSIGNED_SEQ_NO;
                // If this request wait_for_refresh behavior, it is safest to assume a refresh is pending. Theoretically,
                // this can be improved in the future by manually checking that the requested checkpoint has already been refresh.
                // However, this will request modifying the engine to surface that information.
                hasRefreshPending = needsWaitForRefresh || (indexShard.hasRefreshPending() && checkRefreshPending);
                canMatchSearcher = indexShard.acquireSearcher(Engine.CAN_MATCH_SEARCH_SOURCE);
            }
            try (canMatchSearcher) {
                SearchExecutionContext context = indexService.newSearchExecutionContext(
                    request.shardId().id(),
                    0,
                    canMatchSearcher,
                    request::nowInMillis,
                    request.getClusterAlias(),
                    request.getRuntimeMappings()
                );
                final boolean canMatch = queryStillMatchesAfterRewrite(request, context);
                final MinAndMax<?> minMax;
                if (canMatch || hasRefreshPending) {
                    FieldSortBuilder sortBuilder = FieldSortBuilder.getPrimaryFieldSortOrNull(request.source());
                    minMax = sortBuilder != null ? FieldSortBuilder.getMinMaxOrNull(context, sortBuilder) : null;
                } else {
                    minMax = null;
                }
                return new CanMatchShardResponse(canMatch || hasRefreshPending, minMax);
            }
        } finally {
            Releasables.close(releasable);
        }
    }

    @SuppressWarnings("unchecked")
    public static boolean queryStillMatchesAfterRewrite(ShardSearchRequest request, QueryRewriteContext context) throws IOException {
        Rewriteable.rewrite(request.getRewriteable(), context, false);
        final boolean aliasFilterCanMatch = request.getAliasFilter().getQueryBuilder() instanceof MatchNoneQueryBuilder == false;
        final boolean canMatch;
        if (canRewriteToMatchNone(request.source())) {
            QueryBuilder queryBuilder = request.source().query();
            canMatch = aliasFilterCanMatch && queryBuilder instanceof MatchNoneQueryBuilder == false;
        } else {
            // null query means match_all
            canMatch = aliasFilterCanMatch;
        }
        return canMatch;
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

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void rewriteAndFetchShardRequest(IndexShard shard, ShardSearchRequest request, ActionListener<ShardSearchRequest> listener) {
        ActionListener<Rewriteable> actionListener = listener.wrapFailure((l, r) -> {
            if (request.readerId() != null) {
                l.onResponse(request);
            } else {
                // now we need to check if there is a pending refresh and register
                shard.awaitShardSearchActive(b -> l.onResponse(request));
            }
        });
        // we also do rewrite on the coordinating node (TransportSearchService) but we also need to do it here for BWC as well as
        // AliasFilters that might need to be rewritten. These are edge-cases but we are every efficient doing the rewrite here so it's not
        // adding a lot of overhead
        Rewriteable.rewriteAndFetch(request.getRewriteable(), indicesService.getRewriteContext(request::nowInMillis), actionListener);
    }

    /**
     * Returns a new {@link QueryRewriteContext} with the given {@code now} provider
     */
    public QueryRewriteContext getRewriteContext(LongSupplier nowInMillis) {
        return indicesService.getRewriteContext(nowInMillis);
    }

    public CoordinatorRewriteContextProvider getCoordinatorRewriteContextProvider(LongSupplier nowInMillis) {
        return indicesService.getCoordinatorRewriteContextProvider(nowInMillis);
    }

    public IndicesService getIndicesService() {
        return indicesService;
    }

    /**
     * Returns a builder for {@link AggregationReduceContext}.
     */
    public AggregationReduceContext.Builder aggReduceContextBuilder(Supplier<Boolean> isCanceled, AggregatorFactories.Builder aggs) {
        return new AggregationReduceContext.Builder() {
            @Override
            public AggregationReduceContext forPartialReduction() {
                return new AggregationReduceContext.ForPartial(bigArrays, scriptService, isCanceled, aggs);
            }

            @Override
            public AggregationReduceContext forFinalReduction() {
                return new AggregationReduceContext.ForFinal(
                    bigArrays,
                    scriptService,
                    isCanceled,
                    aggs,
                    multiBucketConsumerService.create()
                );
            }
        };
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
