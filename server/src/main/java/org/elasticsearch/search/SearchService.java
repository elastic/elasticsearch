
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.search.CanMatchNodeRequest;
import org.elasticsearch.action.search.CanMatchNodeResponse;
import org.elasticsearch.action.search.OnlinePrewarmingService;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.ResolvedExpression;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.CoordinatorRewriteContextProvider;
import org.elasticsearch.index.query.InnerHitContextBuilder;
import org.elasticsearch.index.query.InnerHitsRewriteContext;
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
import org.elasticsearch.indices.cluster.IndexRemovalReason;
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.AggregationContext.ProductionAggregationContext;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SubSearchSourceBuilder;
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
import org.elasticsearch.search.query.SearchTimeoutException;
import org.elasticsearch.search.rank.feature.RankFeatureResult;
import org.elasticsearch.search.rank.feature.RankFeatureShardPhase;
import org.elasticsearch.search.rank.feature.RankFeatureShardRequest;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.MinAndMax;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.Scheduler.Cancellable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.Transports;

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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.TransportVersions.ERROR_TRACE_IN_TRANSPORT_HEADER;
import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.core.TimeValue.timeValueHours;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueMinutes;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.search.rank.feature.RankFeatureShardPhase.EMPTY_RESULT;

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

    // This setting is only registered on tests to force concurrent search even when segments contains very few documents.
    public static final Setting<Integer> MINIMUM_DOCS_PER_SLICE = Setting.intSetting(
        "search.minimum_docs_per_slice",
        50_000,
        1,
        Property.NodeScope
    );

    public static final Setting<Boolean> SEARCH_WORKER_THREADS_ENABLED = Setting.boolSetting(
        "search.worker_threads_enabled",
        true,
        Property.NodeScope,
        Property.Dynamic,
        Property.DeprecatedWarning
    );

    public static final Setting<Boolean> QUERY_PHASE_PARALLEL_COLLECTION_ENABLED = Setting.boolSetting(
        "search.query_phase_parallel_collection_enabled",
        true,
        Property.NodeScope,
        Property.Dynamic
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
        ByteSizeValue.of(10, ByteSizeUnit.MB),
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Boolean> CCS_COLLECT_TELEMETRY = Setting.boolSetting(
        "search.ccs.collect_telemetry",
        true,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Boolean> BATCHED_QUERY_PHASE = Setting.boolSetting(
        "search.batched_query_phase",
        true,
        Property.Dynamic,
        Property.NodeScope
    );

    // This setting ensures that we skip online prewarming tasks if the queuing in the search thread pool
    // reaches the configured factor X number of max threads in the search thread pool, such that
    // the system has a chance to catch up and prewarming doesn't take over the network bandwidth
    public static final Setting<Integer> PREWARMING_THRESHOLD_THREADPOOL_SIZE_FACTOR_POOL_SIZE = Setting.intSetting(
        "search.online_prewarming_threshold_poolsize_factor",
        10, // we will only execute online prewarming if there are less than 10 queued up items/ search thread
        0, // 0 would mean we only execute online prewarming if there's no queuing in the search tp
        Setting.Property.NodeScope
    );

    private static final boolean BATCHED_QUERY_PHASE_FEATURE_FLAG = new FeatureFlag("batched_query_phase").isEnabled();

    /**
     * The size of the buffer used for memory accounting.
     * This buffer is used to locally track the memory accummulated during the execution of
     * a search request before submitting the accumulated value to the circuit breaker.
     */
    public static final Setting<ByteSizeValue> MEMORY_ACCOUNTING_BUFFER_SIZE = Setting.byteSizeSetting(
        "search.memory_accounting_buffer_size",
        ByteSizeValue.of(1, ByteSizeUnit.MB),
        ByteSizeValue.of(1, ByteSizeUnit.MB),
        ByteSizeValue.ofBytes(Long.MAX_VALUE),
        Property.Dynamic,
        Property.NodeScope
    );

    public static final int DEFAULT_SIZE = 10;
    public static final int DEFAULT_FROM = 0;
    private static final StackTraceElement[] EMPTY_STACK_TRACE_ARRAY = new StackTraceElement[0];

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final ScriptService scriptService;

    private final ExecutorSelector executorSelector;

    private final BigArrays bigArrays;

    private final FetchPhase fetchPhase;
    private final CircuitBreaker circuitBreaker;
    private final OnlinePrewarmingService onlinePrewarmingService;
    private final int prewarmingMaxPoolFactorThreshold;
    private volatile Executor searchExecutor;
    private volatile boolean enableQueryPhaseParallelCollection;

    private volatile long defaultKeepAlive;

    private volatile long maxKeepAlive;

    private volatile TimeValue defaultSearchTimeout;

    private volatile boolean batchQueryPhase;

    private final int minimumDocsPerSlice;

    private volatile boolean defaultAllowPartialSearchResults;

    private volatile boolean lowLevelCancellation;

    private volatile int maxOpenScrollContext;

    private volatile boolean enableRewriteAggsToFilterByFilter;

    private volatile long memoryAccountingBufferSize;

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
        CircuitBreakerService circuitBreakerService,
        ExecutorSelector executorSelector,
        Tracer tracer,
        OnlinePrewarmingService onlinePrewarmingService
    ) {
        Settings settings = clusterService.getSettings();
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.scriptService = scriptService;
        this.bigArrays = bigArrays;
        this.fetchPhase = fetchPhase;
        circuitBreaker = circuitBreakerService.getBreaker(CircuitBreaker.REQUEST);
        this.multiBucketConsumerService = new MultiBucketConsumerService(clusterService, settings, circuitBreaker);
        this.executorSelector = executorSelector;
        this.tracer = tracer;
        this.onlinePrewarmingService = onlinePrewarmingService;
        TimeValue keepAliveInterval = KEEPALIVE_INTERVAL_SETTING.get(settings);
        setKeepAlives(DEFAULT_KEEPALIVE_SETTING.get(settings), MAX_KEEPALIVE_SETTING.get(settings));

        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                DEFAULT_KEEPALIVE_SETTING,
                MAX_KEEPALIVE_SETTING,
                this::setKeepAlives,
                SearchService::validateKeepAlives
            );

        this.keepAliveReaper = threadPool.scheduleWithFixedDelay(new Reaper(), keepAliveInterval, threadPool.executor(Names.SEARCH));

        defaultSearchTimeout = DEFAULT_SEARCH_TIMEOUT_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DEFAULT_SEARCH_TIMEOUT_SETTING, this::setDefaultSearchTimeout);

        minimumDocsPerSlice = MINIMUM_DOCS_PER_SLICE.get(settings);

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

        if (SEARCH_WORKER_THREADS_ENABLED.get(settings)) {
            searchExecutor = threadPool.executor(Names.SEARCH);
        }

        clusterService.getClusterSettings().addSettingsUpdateConsumer(SEARCH_WORKER_THREADS_ENABLED, this::setEnableSearchWorkerThreads);

        enableQueryPhaseParallelCollection = QUERY_PHASE_PARALLEL_COLLECTION_ENABLED.get(settings);
        if (BATCHED_QUERY_PHASE_FEATURE_FLAG) {
            batchQueryPhase = BATCHED_QUERY_PHASE.get(settings);
        } else {
            batchQueryPhase = false;
        }
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(QUERY_PHASE_PARALLEL_COLLECTION_ENABLED, this::setEnableQueryPhaseParallelCollection);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(BATCHED_QUERY_PHASE, bulkExecuteQueryPhase -> this.batchQueryPhase = bulkExecuteQueryPhase);
        memoryAccountingBufferSize = MEMORY_ACCOUNTING_BUFFER_SIZE.get(settings).getBytes();
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(MEMORY_ACCOUNTING_BUFFER_SIZE, newValue -> this.memoryAccountingBufferSize = newValue.getBytes());
        prewarmingMaxPoolFactorThreshold = PREWARMING_THRESHOLD_THREADPOOL_SIZE_FACTOR_POOL_SIZE.get(settings);
    }

    public CircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    private void setEnableSearchWorkerThreads(boolean enableSearchWorkerThreads) {
        if (enableSearchWorkerThreads) {
            searchExecutor = threadPool.executor(Names.SEARCH);
        } else {
            searchExecutor = null;
        }
    }

    private void setEnableQueryPhaseParallelCollection(boolean enableQueryPhaseParallelCollection) {
        this.enableQueryPhaseParallelCollection = enableQueryPhaseParallelCollection;
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

    public boolean batchQueryPhase() {
        return batchQueryPhase;
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
        if (routing.isRelocationTarget() == false && routing.recoverySource() != RecoverySource.EmptyStoreRecoverySource.INSTANCE) {
            freeAllContextsForShard(routing.shardId());
        }
    }

    protected void putReaderContext(ReaderContext context) {
        final long id = context.id().getId();
        final ReaderContext previous = activeReaders.put(id, context);
        assert previous == null;
        // ensure that if we race against afterIndexRemoved, we remove the context from the active list.
        // this is important to ensure store can be cleaned up, in particular if the search is a scroll with a long timeout.
        final Index index = context.indexShard().shardId().getIndex();
        if (indicesService.hasIndex(index) == false) {
            removeReaderContext(id);
            throw new IndexNotFoundException(index);
        }
    }

    protected ReaderContext removeReaderContext(long id) {
        if (logger.isTraceEnabled()) {
            logger.trace("removing reader context [{}]", id);
        }
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

    /**
     * Wraps the listener to ensure errors are logged and to avoid sending
     * StackTraces back to the coordinating node if the `error_trace` header is
     * set to {@code false}. Upon reading, we default to {@code true} to maintain
     * the same behavior as before the change, due to older nodes not being able
     * to specify whether they need stack traces.
     *
     * @param <T>            the type of the response
     * @param listener       the action listener to be wrapped
     * @param version        channel version of the request
     * @param nodeId         id of the current node
     * @param shardId        id of the shard being searched
     * @param taskId         id of the task being executed
     * @param threadPool     with context where to write the new header
     * @return the wrapped action listener
     */
    static <T> ActionListener<T> wrapListenerForErrorHandling(
        ActionListener<T> listener,
        TransportVersion version,
        String nodeId,
        ShardId shardId,
        long taskId,
        ThreadPool threadPool
    ) {
        final boolean header;
        if (version.onOrAfter(ERROR_TRACE_IN_TRANSPORT_HEADER) && threadPool.getThreadContext() != null) {
            header = Boolean.parseBoolean(threadPool.getThreadContext().getHeaderOrDefault("error_trace", "false"));
        } else {
            header = true;
        }
        return listener.delegateResponse((l, e) -> {
            org.apache.logging.log4j.util.Supplier<String> messageSupplier = () -> format(
                "[%s]%s: failed to execute search request for task [%d]",
                nodeId,
                shardId,
                taskId
            );
            // Keep this logic aligned with that of SUPPRESSED_ERROR_LOGGER in RestResponse
            if (ExceptionsHelper.status(e).getStatus() < 500 || ExceptionsHelper.isNodeOrShardUnavailableTypeException(e)) {
                logger.debug(messageSupplier, e);
            } else {
                logger.warn(messageSupplier, e);
            }

            if (header == false) {
                ExceptionsHelper.unwrapCausesAndSuppressed(e, err -> {
                    err.setStackTrace(EMPTY_STACK_TRACE_ARRAY);
                    return false;
                });
            }

            l.onFailure(e);
        });
    }

    public void executeDfsPhase(ShardSearchRequest request, SearchShardTask task, ActionListener<SearchPhaseResult> listener) {
        listener = wrapListenerForErrorHandling(
            listener,
            request.getChannelVersion(),
            clusterService.localNode().getId(),
            request.shardId(),
            task.getId(),
            threadPool
        );
        final IndexShard shard = getShard(request);
        rewriteAndFetchShardRequest(shard, request, listener.delegateFailure((l, rewritten) -> {
            // fork the execution in the search thread pool
            ensureAfterSeqNoRefreshed(shard, request, () -> executeDfsPhase(request, task), l);
        }));
    }

    private DfsSearchResult executeDfsPhase(ShardSearchRequest request, SearchShardTask task) throws IOException {
        ReaderContext readerContext = createOrGetReaderContext(request);
        try (@SuppressWarnings("unused") // withScope call is necessary to instrument search execution
        Releasable scope = tracer.withScope(task);
            Releasable ignored = readerContext.markAsUsed(getKeepAlive(request));
            SearchContext context = createContext(readerContext, request, task, ResultsType.DFS, false)
        ) {
            DfsPhase.execute(context);
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

    public void executeQueryPhase(ShardSearchRequest request, CancellableTask task, ActionListener<SearchPhaseResult> listener) {
        assert request.canReturnNullResponseIfMatchNoDocs() == false || request.numberOfShards() > 1
            : "empty responses require more than one shard";
        final IndexShard shard = getShard(request);
        rewriteAndFetchShardRequest(
            shard,
            request,
            wrapListenerForErrorHandling(
                listener,
                request.getChannelVersion(),
                clusterService.localNode().getId(),
                request.shardId(),
                task.getId(),
                threadPool
            ).delegateFailure((l, orig) -> {
                // check if we can shortcut the query phase entirely.
                if (orig.canReturnNullResponseIfMatchNoDocs()) {
                    assert orig.scroll() == null;
                    ShardSearchRequest clone = new ShardSearchRequest(orig);
                    CanMatchContext canMatchContext = new CanMatchContext(
                        clone,
                        indicesService::indexServiceSafe,
                        this::findReaderContext,
                        defaultKeepAlive,
                        maxKeepAlive
                    );
                    CanMatchShardResponse canMatchResp = canMatch(canMatchContext, false);
                    if (canMatchResp.canMatch() == false) {
                        l.onResponse(QuerySearchResult.nullInstance());
                        return;
                    }
                }
                // TODO: i think it makes sense to always do a canMatch here and
                // return an empty response (not null response) in case canMatch is false?
                ensureAfterSeqNoRefreshed(shard, orig, () -> executeQueryPhase(orig, task), l);
            })
        );
    }

    private <T extends RefCounted> void ensureAfterSeqNoRefreshed(
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
                // we successfully submitted the async task to the search pool so let's prewarm the shard
                if (isExecutorQueuedBeyondPrewarmingFactor(executor, prewarmingMaxPoolFactorThreshold) == false) {
                    onlinePrewarmingService.prewarm(shard);
                }
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
                    var shardTarget = new SearchShardTarget(
                        shard.routingEntry().currentNodeId(),
                        shard.shardId(),
                        request.getClusterAlias()
                    );
                    var message = LoggerMessageFormat.format("Wait for seq_no [{}] refreshed timed out [{}]", waitForCheckpoint, timeout);
                    listener.onFailure(new SearchTimeoutException(shardTarget, message));
                }
            }, timeout, EsExecutors.DIRECT_EXECUTOR_SERVICE);

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
                            return EsExecutors.DIRECT_EXECUTOR_SERVICE;
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
                        // we successfully submitted the async task to the search pool so let's prewarm the shard
                        if (isExecutorQueuedBeyondPrewarmingFactor(executor, prewarmingMaxPoolFactorThreshold) == false) {
                            onlinePrewarmingService.prewarm(shard);
                        }
                    }
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Checks if the executor is queued beyond the prewarming factor threshold, relative to the
     * number of threads in the pool.
     * This is used to determine if we should prewarm the shard - i.e. if the executor doesn't
     * contain queued tasks beyond the prewarming factor threshold X max pool size.
     *
     * @param searchOperationsExecutor the executor that executes the search operations
     * @param prewarmingMaxPoolFactorThreshold maximum number of queued up items / thread in the search pool
     */
    // visible for testing
    static boolean isExecutorQueuedBeyondPrewarmingFactor(Executor searchOperationsExecutor, int prewarmingMaxPoolFactorThreshold) {
        if (searchOperationsExecutor instanceof ThreadPoolExecutor tpe) {
            return (tpe.getMaximumPoolSize() * prewarmingMaxPoolFactorThreshold) < tpe.getQueue().size();
        } else {
            logger.trace(
                "received executor [{}] that we can't inspect for queueing. allowing online prewarming for all searches",
                searchOperationsExecutor
            );
            return false;
        }
    }

    private IndexShard getShard(ShardSearchRequest request) {
        final ShardSearchContextId contextId = request.readerId();
        if (contextId != null && sessionId.equals(contextId.getSessionId())) {
            final ReaderContext readerContext = activeReaders.get(contextId.getId());
            if (readerContext != null) {
                return readerContext.indexShard();
            }
        }
        return indicesService.indexServiceSafe(request.shardId().getIndex()).getShard(request.shardId().id());
    }

    private static <T extends RefCounted> void runAsync(
        Executor executor,
        CheckedSupplier<T, Exception> executable,
        ActionListener<T> listener
    ) {
        executor.execute(ActionRunnable.supplyAndDecRef(listener, executable));
    }

    /**
     * The returned {@link SearchPhaseResult} will have had its ref count incremented by this method.
     * It is the responsibility of the caller to ensure that the ref count is correctly decremented
     * when the object is no longer needed.
     */
    private SearchPhaseResult executeQueryPhase(ShardSearchRequest request, CancellableTask task) throws Exception {
        final ReaderContext readerContext = createOrGetReaderContext(request);
        try (
            Releasable scope = tracer.withScope(task);
            Releasable ignored = readerContext.markAsUsed(getKeepAlive(request));
            SearchContext context = createContext(readerContext, request, task, ResultsType.QUERY, true)
        ) {
            tracer.startTrace("executeQueryPhase", Map.of());
            final long afterQueryTime;
            final long beforeQueryTime = System.nanoTime();
            var opsListener = context.indexShard().getSearchOperationListener();
            opsListener.onPreQueryPhase(context);
            try {
                loadOrExecuteQueryPhase(request, context);
                if (context.queryResult().hasSearchContext() == false && readerContext.singleSession()) {
                    freeReaderContext(readerContext.id());
                }
                afterQueryTime = System.nanoTime();
                opsListener.onQueryPhase(context, afterQueryTime - beforeQueryTime);
                opsListener = null;
            } finally {
                if (opsListener != null) {
                    opsListener.onFailedQueryPhase(context);
                }
                tracer.stopTrace(task);
            }
            if (request.numberOfShards() == 1 && (request.source() == null || request.source().rankBuilder() == null)) {
                // we already have query results, but we can run fetch at the same time
                context.addFetchResult();
                return executeFetchPhase(readerContext, context, afterQueryTime);
            } else {
                // Pass the rescoreDocIds to the queryResult to send them the coordinating node and receive them back in the fetch phase.
                // We also pass the rescoreDocIds to the LegacyReaderContext in case the search state needs to stay in the data node.
                final RescoreDocIds rescoreDocIds = context.rescoreDocIds();
                context.queryResult().setRescoreDocIds(rescoreDocIds);
                readerContext.setRescoreDocIds(rescoreDocIds);
                // inc-ref query result because we close the SearchContext that references it in this try-with-resources block
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

    public void executeRankFeaturePhase(RankFeatureShardRequest request, SearchShardTask task, ActionListener<RankFeatureResult> listener) {
        final ReaderContext readerContext = findReaderContext(request.contextId(), request);
        final ShardSearchRequest shardSearchRequest = readerContext.getShardSearchRequest(request.getShardSearchRequest());
        listener = wrapListenerForErrorHandling(
            listener,
            shardSearchRequest.getChannelVersion(),
            clusterService.localNode().getId(),
            shardSearchRequest.shardId(),
            task.getId(),
            threadPool
        );
        final Releasable markAsUsed = readerContext.markAsUsed(getKeepAlive(shardSearchRequest));
        runAsync(getExecutor(readerContext.indexShard()), () -> {
            try (SearchContext searchContext = createContext(readerContext, shardSearchRequest, task, ResultsType.RANK_FEATURE, false)) {
                int[] docIds = request.getDocIds();
                if (docIds == null || docIds.length == 0) {
                    searchContext.rankFeatureResult().shardResult(EMPTY_RESULT);
                    searchContext.rankFeatureResult().incRef();
                    return searchContext.rankFeatureResult();
                }
                RankFeatureShardPhase.prepareForFetch(searchContext, request);
                fetchPhase.execute(searchContext, docIds, null);
                RankFeatureShardPhase.processFetch(searchContext);
                var rankFeatureResult = searchContext.rankFeatureResult();
                rankFeatureResult.incRef();
                return rankFeatureResult;
            } catch (Exception e) {
                assert TransportActions.isShardNotAvailableException(e) == false : new AssertionError(e);
                // we handle the failure in the failure listener below
                throw e;
            }
        }, wrapFailureListener(listener, readerContext, markAsUsed));
    }

    private QueryFetchSearchResult executeFetchPhase(ReaderContext reader, SearchContext context, long afterQueryTime) {
        var opsListener = context.indexShard().getSearchOperationListener();
        try (Releasable scope = tracer.withScope(context.getTask());) {
            opsListener.onPreFetchPhase(context);
            fetchPhase.execute(context, shortcutDocIdsToLoad(context), null);
            if (reader.singleSession()) {
                freeReaderContext(reader.id());
            }
            opsListener.onFetchPhase(context, System.nanoTime() - afterQueryTime);
            opsListener = null;
        } finally {
            if (opsListener != null) {
                opsListener.onFailedFetchPhase(context);
            }
        }
        // This will incRef the QuerySearchResult when it gets created
        return QueryFetchSearchResult.of(context.queryResult(), context.fetchResult());
    }

    public void executeQueryPhase(
        InternalScrollSearchRequest request,
        SearchShardTask task,
        ActionListener<ScrollQuerySearchResult> listener,
        TransportVersion version
    ) {
        final LegacyReaderContext readerContext = (LegacyReaderContext) findReaderContext(request.contextId(), request);
        listener = wrapListenerForErrorHandling(
            listener,
            version,
            clusterService.localNode().getId(),
            readerContext.indexShard().shardId(),
            task.getId(),
            threadPool
        );
        final Releasable markAsUsed;
        try {
            markAsUsed = readerContext.markAsUsed(getScrollKeepAlive(request.scroll()));
        } catch (Exception e) {
            // We need to release the reader context of the scroll when we hit any exception (here the keep_alive can be too large)
            freeReaderContext(readerContext.id());
            throw e;
        }
        Executor executor = getExecutor(readerContext.indexShard());
        runAsync(executor, () -> {
            final ShardSearchRequest shardSearchRequest = readerContext.getShardSearchRequest(null);
            try (SearchContext searchContext = createContext(readerContext, shardSearchRequest, task, ResultsType.QUERY, false);) {
                var opsListener = searchContext.indexShard().getSearchOperationListener();
                final long beforeQueryTime = System.nanoTime();
                opsListener.onPreQueryPhase(searchContext);
                try {
                    searchContext.searcher().setAggregatedDfs(readerContext.getAggregatedDfs(null));
                    processScroll(request, searchContext);
                    QueryPhase.execute(searchContext);
                    opsListener.onQueryPhase(searchContext, System.nanoTime() - beforeQueryTime);
                    opsListener = null;
                } finally {
                    if (opsListener != null) {
                        opsListener.onFailedQueryPhase(searchContext);
                    }
                }
                readerContext.setRescoreDocIds(searchContext.rescoreDocIds());
                // ScrollQuerySearchResult will incRef the QuerySearchResult when it gets constructed.
                return new ScrollQuerySearchResult(searchContext.queryResult(), searchContext.shardTarget());
            } catch (Exception e) {
                logger.trace("Query phase failed", e);
                // we handle the failure in the failure listener below
                throw e;
            }
        }, wrapFailureListener(listener, readerContext, markAsUsed));
        // we successfully submitted the async task to the search pool so let's prewarm the shard
        if (isExecutorQueuedBeyondPrewarmingFactor(executor, prewarmingMaxPoolFactorThreshold) == false) {
            onlinePrewarmingService.prewarm(readerContext.indexShard());
        }
    }

    /**
     * The returned {@link SearchPhaseResult} will have had its ref count incremented by this method.
     * It is the responsibility of the caller to ensure that the ref count is correctly decremented
     * when the object is no longer needed.
     */
    public void executeQueryPhase(
        QuerySearchRequest request,
        SearchShardTask task,
        ActionListener<QuerySearchResult> listener,
        TransportVersion version
    ) {
        final ReaderContext readerContext = findReaderContext(request.contextId(), request.shardSearchRequest());
        final ShardSearchRequest shardSearchRequest = readerContext.getShardSearchRequest(request.shardSearchRequest());
        listener = wrapListenerForErrorHandling(
            listener,
            version,
            clusterService.localNode().getId(),
            shardSearchRequest.shardId(),
            task.getId(),
            threadPool
        );
        final Releasable markAsUsed = readerContext.markAsUsed(getKeepAlive(shardSearchRequest));
        rewriteAndFetchShardRequest(readerContext.indexShard(), shardSearchRequest, listener.delegateFailure((l, rewritten) -> {
            // fork the execution in the search thread pool
            Executor executor = getExecutor(readerContext.indexShard());
            runAsync(executor, () -> {
                readerContext.setAggregatedDfs(request.dfs());
                try (SearchContext searchContext = createContext(readerContext, shardSearchRequest, task, ResultsType.QUERY, true);) {
                    final QuerySearchResult queryResult;
                    var opsListener = searchContext.indexShard().getSearchOperationListener();
                    final long before = System.nanoTime();
                    opsListener.onPreQueryPhase(searchContext);
                    try {
                        searchContext.searcher().setAggregatedDfs(request.dfs());
                        QueryPhase.execute(searchContext);
                        queryResult = searchContext.queryResult();
                        if (queryResult.hasSearchContext() == false && readerContext.singleSession()) {
                            // no hits, we can release the context since there will be no fetch phase
                            freeReaderContext(readerContext.id());
                        }
                        opsListener.onQueryPhase(searchContext, System.nanoTime() - before);
                        opsListener = null;
                    } finally {
                        if (opsListener != null) {
                            opsListener.onFailedQueryPhase(searchContext);
                        }
                    }
                    // Pass the rescoreDocIds to the queryResult to send them the coordinating node
                    // and receive them back in the fetch phase.
                    // We also pass the rescoreDocIds to the LegacyReaderContext in case the search state needs to stay in the data node.
                    final RescoreDocIds rescoreDocIds = searchContext.rescoreDocIds();
                    queryResult.setRescoreDocIds(rescoreDocIds);
                    readerContext.setRescoreDocIds(rescoreDocIds);
                    // inc-ref query result because we close the SearchContext that references it in this try-with-resources block
                    queryResult.incRef();
                    return queryResult;
                } catch (Exception e) {
                    assert TransportActions.isShardNotAvailableException(e) == false : new AssertionError(e);
                    logger.trace("Query phase failed", e);
                    // we handle the failure in the failure listener below
                    throw e;
                }
            }, wrapFailureListener(l, readerContext, markAsUsed));
            // we successfully submitted the async task to the search pool so let's prewarm the shard
            if (isExecutorQueuedBeyondPrewarmingFactor(executor, prewarmingMaxPoolFactorThreshold) == false) {
                onlinePrewarmingService.prewarm(readerContext.indexShard());
            }
        }));
    }

    private Executor getExecutor(IndexShard indexShard) {
        assert indexShard != null;
        final String executorName;
        if (indexShard.isSystem()) {
            executorName = executorSelector.executorForSearch(indexShard.shardId().getIndexName());
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
            try (SearchContext searchContext = createContext(readerContext, shardSearchRequest, task, ResultsType.FETCH, false);) {
                var opsListener = readerContext.indexShard().getSearchOperationListener();
                final long beforeQueryTime = System.nanoTime();
                final long afterQueryTime;
                try {
                    opsListener.onPreQueryPhase(searchContext);
                    searchContext.assignRescoreDocIds(readerContext.getRescoreDocIds(null));
                    searchContext.searcher().setAggregatedDfs(readerContext.getAggregatedDfs(null));
                    processScroll(request, searchContext);
                    searchContext.addQueryResult();
                    QueryPhase.execute(searchContext);
                    afterQueryTime = System.nanoTime();
                    opsListener.onQueryPhase(searchContext, afterQueryTime - beforeQueryTime);
                    opsListener = null;
                } finally {
                    if (opsListener != null) {
                        opsListener.onFailedQueryPhase(searchContext);
                    }
                }
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

    public void executeFetchPhase(ShardFetchRequest request, CancellableTask task, ActionListener<FetchSearchResult> listener) {
        final ReaderContext readerContext = findReaderContext(request.contextId(), request);
        final ShardSearchRequest shardSearchRequest = readerContext.getShardSearchRequest(request.getShardSearchRequest());
        final Releasable markAsUsed = readerContext.markAsUsed(getKeepAlive(shardSearchRequest));
        rewriteAndFetchShardRequest(readerContext.indexShard(), shardSearchRequest, listener.delegateFailure((l, rewritten) -> {
            runAsync(getExecutor(readerContext.indexShard()), () -> {
                try (SearchContext searchContext = createContext(readerContext, rewritten, task, ResultsType.FETCH, false)) {
                    if (request.lastEmittedDoc() != null) {
                        searchContext.scrollContext().lastEmittedDoc = request.lastEmittedDoc();
                    }
                    searchContext.assignRescoreDocIds(readerContext.getRescoreDocIds(request.getRescoreDocIds()));
                    searchContext.searcher().setAggregatedDfs(readerContext.getAggregatedDfs(request.getAggregatedDfs()));
                    final long startTime = System.nanoTime();
                    var opsListener = searchContext.indexShard().getSearchOperationListener();
                    opsListener.onPreFetchPhase(searchContext);
                    try {
                        fetchPhase.execute(searchContext, request.docIds(), request.getRankDocks());
                        if (readerContext.singleSession()) {
                            freeReaderContext(request.contextId());
                        }
                        opsListener.onFetchPhase(searchContext, System.nanoTime() - startTime);
                        opsListener = null;
                    } finally {
                        if (opsListener != null) {
                            opsListener.onFailedFetchPhase(searchContext);
                        }
                    }
                    var fetchResult = searchContext.fetchResult();
                    // inc-ref fetch result because we close the SearchContext that references it in this try-with-resources block
                    fetchResult.incRef();
                    return fetchResult;
                } catch (Exception e) {
                    assert TransportActions.isShardNotAvailableException(e) == false : new AssertionError(e);
                    // we handle the failure in the failure listener below
                    throw e;
                }
            }, wrapFailureListener(l, readerContext, markAsUsed));
        }));
    }

    protected void checkCancelled(CancellableTask task) {
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
        }
        final long keepAliveInMillis = getKeepAlive(request);
        final IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        final IndexShard shard = indexService.getShard(request.shardId().id());
        return createAndPutReaderContext(request, indexService, shard, shard.acquireSearcherSupplier(), keepAliveInMillis);
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
            final ShardSearchContextId id = new ShardSearchContextId(sessionId, idGenerator.incrementAndGet());
            if (request.scroll() != null) {
                decreaseScrollContexts = openScrollContexts::decrementAndGet;
                if (openScrollContexts.incrementAndGet() > maxOpenScrollContext) {
                    throw new TooManyScrollContextsException(maxOpenScrollContext, MAX_OPEN_SCROLL_CONTEXT.getKey());
                }
                readerContext = new LegacyReaderContext(id, indexService, shard, reader, request, keepAliveInMillis);
                readerContext.addOnClose(decreaseScrollContexts);
                decreaseScrollContexts = null;
            } else {
                readerContext = new ReaderContext(id, indexService, shard, reader, keepAliveInMillis, true);
            }
            reader = null;
            final ReaderContext finalReaderContext = readerContext;
            final SearchOperationListener searchOperationListener = shard.getSearchOperationListener();
            searchOperationListener.onNewReaderContext(finalReaderContext);
            if (finalReaderContext.scrollContext() != null) {
                searchOperationListener.onNewScrollContext(finalReaderContext);
                readerContext.addOnClose(() -> searchOperationListener.onFreeScrollContext(finalReaderContext));
            }
            readerContext.addOnClose(() -> searchOperationListener.onFreeReaderContext(finalReaderContext));
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
        shard.ensureShardSearchActive(ignored -> {
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
        CancellableTask task,
        ResultsType resultsType,
        boolean includeAggregations
    ) throws IOException {
        checkCancelled(task);
        final DefaultSearchContext context = createSearchContext(readerContext, request, defaultSearchTimeout, resultsType);
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
            context.setTask(task);

            context.preProcess();
        } catch (Exception e) {
            context.close();
            throw e;
        }

        return context;
    }

    public SearchContext createSearchContext(ShardSearchRequest request, TimeValue timeout) throws IOException {
        final IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        final IndexShard indexShard = indexService.getShard(request.shardId().getId());
        final Engine.SearcherSupplier reader = indexShard.acquireSearcherSupplier();
        final ShardSearchContextId id = new ShardSearchContextId(sessionId, idGenerator.incrementAndGet());
        try (ReaderContext readerContext = new ReaderContext(id, indexService, indexShard, reader, -1L, true)) {
            DefaultSearchContext searchContext = createSearchContext(readerContext, request, timeout, ResultsType.NONE);
            searchContext.addReleasable(readerContext.markAsUsed(0L));
            return searchContext;
        }
    }

    @SuppressWarnings("unchecked")
    private DefaultSearchContext createSearchContext(
        ReaderContext reader,
        ShardSearchRequest request,
        TimeValue timeout,
        ResultsType resultsType
    ) throws IOException {
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
                threadPool.relativeTimeInMillisSupplier(),
                timeout,
                fetchPhase,
                lowLevelCancellation,
                searchExecutor,
                resultsType,
                enableQueryPhaseParallelCollection,
                minimumDocsPerSlice,
                memoryAccountingBufferSize
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
        logger.trace("freeing reader context [{}]", contextId);
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
        return getKeepAlive(request, defaultKeepAlive, maxKeepAlive);
    }

    private static long getKeepAlive(ShardSearchRequest request, long defaultKeepAlive, long maxKeepAlive) {
        if (request.scroll() != null) {
            return getScrollKeepAlive(request.scroll(), defaultKeepAlive, maxKeepAlive);
        } else if (request.keepAlive() != null) {
            checkKeepAliveLimit(request.keepAlive().millis(), maxKeepAlive);
            return request.keepAlive().getMillis();
        } else {
            return request.readerId() == null ? defaultKeepAlive : -1;
        }
    }

    private long getScrollKeepAlive(TimeValue keepAlive) {
        return getScrollKeepAlive(keepAlive, defaultKeepAlive, maxKeepAlive);
    }

    private static long getScrollKeepAlive(TimeValue keepAlive, long defaultKeepAlive, long maxKeepAlive) {
        if (keepAlive != null) {
            checkKeepAliveLimit(keepAlive.millis(), maxKeepAlive);
            return keepAlive.getMillis();
        }
        return defaultKeepAlive;
    }

    private void checkKeepAliveLimit(long keepAlive) {
        checkKeepAliveLimit(keepAlive, maxKeepAlive);
    }

    private static void checkKeepAliveLimit(long keepAlive, long maxKeepAlive) {
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

    private void parseSource(DefaultSearchContext context, SearchSourceBuilder source, boolean includeAggregations) throws IOException {
        // nothing to parse...
        if (source == null) {
            return;
        }
        SearchShardTarget shardTarget = context.shardTarget();
        SearchExecutionContext searchExecutionContext = context.getSearchExecutionContext();
        context.from(source.from());
        context.size(source.size());
        Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        QueryBuilder query = source.query();
        InnerHitsRewriteContext innerHitsRewriteContext = new InnerHitsRewriteContext(
            context.getSearchExecutionContext().getParserConfig(),
            context::getRelativeTimeInMillis
        );
        if (query != null) {
            QueryBuilder rewrittenForInnerHits = Rewriteable.rewrite(query, innerHitsRewriteContext, true);
            if (false == source.skipInnerHits()) {
                InnerHitContextBuilder.extractInnerHits(rewrittenForInnerHits, innerHitBuilders);
            }
            searchExecutionContext.setAliasFilter(context.request().getAliasFilter().getQueryBuilder());
            context.parsedQuery(searchExecutionContext.toQuery(query));
        }
        if (source.postFilter() != null) {
            QueryBuilder rewrittenForInnerHits = Rewriteable.rewrite(source.postFilter(), innerHitsRewriteContext, true);
            if (false == source.skipInnerHits()) {
                InnerHitContextBuilder.extractInnerHits(rewrittenForInnerHits, innerHitBuilders);
            }
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
                clusterService.getClusterSettings(),
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
            context.addAggregationContext(aggContext);
            try {
                final AggregatorFactories factories = source.aggregations().build(aggContext, null);
                context.aggregations(
                    new SearchContextAggregations(factories, () -> aggReduceContextBuilder(context::isCancelled, source.aggregations()))
                );
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
            String collapseField = source.collapse() != null ? source.collapse().getField() : null;
            FieldDoc fieldDoc = SearchAfterBuilder.buildFieldDoc(context.sort(), source.searchAfter(), collapseField);
            context.searchAfter(fieldDoc);
        }

        if (source.slice() != null) {
            context.sliceBuilder(source.slice());
        }

        if (source.storedFields() != null) {
            context.storedFieldsContext(source.storedFields());
        }

        if (source.collapse() != null) {
            final CollapseContext collapseContext = source.collapse().build(searchExecutionContext);
            context.collapse(collapseContext);
        }

        if (source.rankBuilder() != null) {
            List<Query> queries = new ArrayList<>();
            for (SubSearchSourceBuilder subSearchSourceBuilder : source.subSearches()) {
                queries.add(subSearchSourceBuilder.toSearchQuery(context.getSearchExecutionContext()));
            }
            context.queryPhaseRankShardContext(source.rankBuilder().buildQueryPhaseShardContext(queries, context.from()));
        }
    }

    /**
     * Shortcut ids to load, we load only "from" and up to "size". The phase controller
     * handles this as well since the result is always size * shards for Q_T_F
     */
    private static int[] shortcutDocIdsToLoad(SearchContext context) {
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
        return docIdsToLoad;
    }

    private static void processScroll(InternalScrollSearchRequest request, SearchContext context) {
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

    public long getDefaultKeepAliveInMillis() {
        return defaultKeepAlive;
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
        RANK_FEATURE {
            @Override
            void addResultsObject(SearchContext context) {
                context.addRankFeatureResult();
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

    class Reaper extends AbstractRunnable {
        @Override
        protected void doRun() {
            assert Transports.assertNotTransportThread("closing contexts may do IO, e.g. deleting dangling files")
                && ThreadPool.assertNotScheduleThread("closing contexts may do IO, e.g. deleting dangling files");
            for (ReaderContext context : activeReaders.values()) {
                if (context.isExpired()) {
                    logger.debug("freeing search context [{}]", context.id());
                    freeReaderContext(context.id());
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("unexpected error when freeing search contexts", e);
            assert false : e;
        }

        @Override
        public void onRejection(Exception e) {
            if (e instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown()) {
                logger.debug("rejected execution when freeing search contexts");
            } else {
                onFailure(e);
            }
        }

        @Override
        public boolean isForceExecution() {
            // mustn't reject this task even if the queue is full
            return true;
        }
    }

    public AliasFilter buildAliasFilter(ProjectState state, String index, Set<ResolvedExpression> resolvedExpressions) {
        return indicesService.buildAliasFilter(state, index, resolvedExpressions);
    }

    public void canMatch(CanMatchNodeRequest request, ActionListener<CanMatchNodeResponse> listener) {
        var shardLevelRequests = request.getShardLevelRequests();
        final List<CanMatchNodeResponse.ResponseOrFailure> responses = new ArrayList<>(shardLevelRequests.size());
        for (var shardLevelRequest : shardLevelRequests) {
            try {
                // TODO remove the exception handling as it's now in canMatch itself
                responses.add(new CanMatchNodeResponse.ResponseOrFailure(canMatch(request.createShardSearchRequest(shardLevelRequest))));
            } catch (Exception e) {
                responses.add(new CanMatchNodeResponse.ResponseOrFailure(e));
            }
        }
        listener.onResponse(new CanMatchNodeResponse(responses));
    }

    /**
     * This method uses a lightweight searcher without wrapping (i.e., not open a full reader on frozen indices) to rewrite the query
     * to check if the query can match any documents. This method can have false positives while if it returns {@code false} the query
     * won't match any documents on the current shard. Exceptions are handled within the method, and never re-thrown.
     */
    public CanMatchShardResponse canMatch(ShardSearchRequest request) {
        CanMatchContext canMatchContext = new CanMatchContext(
            request,
            indicesService::indexServiceSafe,
            this::findReaderContext,
            defaultKeepAlive,
            maxKeepAlive
        );
        return canMatch(canMatchContext, true);
    }

    static class CanMatchContext {
        private final ShardSearchRequest request;
        private final Function<Index, IndexService> indexServiceLookup;
        private final BiFunction<ShardSearchContextId, TransportRequest, ReaderContext> findReaderContext;
        private final long defaultKeepAlive;
        private final long maxKeepAlive;

        private IndexService indexService;

        CanMatchContext(
            ShardSearchRequest request,
            Function<Index, IndexService> indexServiceLookup,
            BiFunction<ShardSearchContextId, TransportRequest, ReaderContext> findReaderContext,
            long defaultKeepAlive,
            long maxKeepAlive
        ) {
            this.request = request;
            this.indexServiceLookup = indexServiceLookup;
            this.findReaderContext = findReaderContext;
            this.defaultKeepAlive = defaultKeepAlive;
            this.maxKeepAlive = maxKeepAlive;
        }

        long getKeepAlive() {
            return SearchService.getKeepAlive(request, defaultKeepAlive, maxKeepAlive);
        }

        ReaderContext findReaderContext() {
            return findReaderContext.apply(request.readerId(), request);
        }

        QueryRewriteContext getQueryRewriteContext(IndexService indexService) {
            return indexService.newQueryRewriteContext(request::nowInMillis, request.getRuntimeMappings(), request.getClusterAlias());
        }

        SearchExecutionContext getSearchExecutionContext(Engine.Searcher searcher) {
            return getIndexService().newSearchExecutionContext(
                request.shardId().id(),
                0,
                searcher,
                request::nowInMillis,
                request.getClusterAlias(),
                request.getRuntimeMappings()
            );
        }

        IndexShard getShard() {
            return getIndexService().getShard(request.shardId().getId());
        }

        IndexService getIndexService() {
            if (this.indexService == null) {
                this.indexService = indexServiceLookup.apply(request.shardId().getIndex());
            }
            return this.indexService;
        }
    }

    static CanMatchShardResponse canMatch(CanMatchContext canMatchContext, boolean checkRefreshPending) {
        assert canMatchContext.request.searchType() == SearchType.QUERY_THEN_FETCH
            : "unexpected search type: " + canMatchContext.request.searchType();
        Releasable releasable = null;
        try {
            IndexService indexService;
            final boolean hasRefreshPending;
            final Engine.Searcher canMatchSearcher;
            if (canMatchContext.request.readerId() != null) {
                hasRefreshPending = false;
                ReaderContext readerContext;
                Engine.Searcher searcher;
                try {
                    readerContext = canMatchContext.findReaderContext();
                    releasable = readerContext.markAsUsed(canMatchContext.getKeepAlive());
                    indexService = readerContext.indexService();
                    QueryRewriteContext queryRewriteContext = canMatchContext.getQueryRewriteContext(indexService);
                    if (queryStillMatchesAfterRewrite(canMatchContext.request, queryRewriteContext) == false) {
                        return new CanMatchShardResponse(false, null);
                    }
                    searcher = readerContext.acquireSearcher(Engine.CAN_MATCH_SEARCH_SOURCE);
                } catch (SearchContextMissingException e) {
                    final String searcherId = canMatchContext.request.readerId().getSearcherId();
                    if (searcherId == null) {
                        return new CanMatchShardResponse(true, null);
                    }
                    if (queryStillMatchesAfterRewrite(
                        canMatchContext.request,
                        canMatchContext.getQueryRewriteContext(canMatchContext.getIndexService())
                    ) == false) {
                        return new CanMatchShardResponse(false, null);
                    }
                    final Engine.SearcherSupplier searcherSupplier = canMatchContext.getShard().acquireSearcherSupplier();
                    if (searcherId.equals(searcherSupplier.getSearcherId()) == false) {
                        searcherSupplier.close();
                        return new CanMatchShardResponse(true, null);
                    }
                    releasable = searcherSupplier;
                    searcher = searcherSupplier.acquireSearcher(Engine.CAN_MATCH_SEARCH_SOURCE);
                }
                canMatchSearcher = searcher;
            } else {
                if (queryStillMatchesAfterRewrite(
                    canMatchContext.request,
                    canMatchContext.getQueryRewriteContext(canMatchContext.getIndexService())
                ) == false) {
                    return new CanMatchShardResponse(false, null);
                }
                boolean needsWaitForRefresh = canMatchContext.request.waitForCheckpoint() != UNASSIGNED_SEQ_NO;
                // If this request wait_for_refresh behavior, it is safest to assume a refresh is pending. Theoretically,
                // this can be improved in the future by manually checking that the requested checkpoint has already been refresh.
                // However, this will request modifying the engine to surface that information.
                IndexShard indexShard = canMatchContext.getShard();
                hasRefreshPending = needsWaitForRefresh || (indexShard.hasRefreshPending() && checkRefreshPending);
                canMatchSearcher = indexShard.acquireSearcher(Engine.CAN_MATCH_SEARCH_SOURCE);
            }
            try (canMatchSearcher) {
                SearchExecutionContext context = canMatchContext.getSearchExecutionContext(canMatchSearcher);
                final boolean canMatch = queryStillMatchesAfterRewrite(canMatchContext.request, context);
                if (canMatch || hasRefreshPending) {
                    FieldSortBuilder sortBuilder = FieldSortBuilder.getPrimaryFieldSortOrNull(canMatchContext.request.source());
                    final MinAndMax<?> minMax = sortBuilder != null ? FieldSortBuilder.getMinMaxOrNull(context, sortBuilder) : null;
                    return new CanMatchShardResponse(true, minMax);
                }
                return new CanMatchShardResponse(false, null);
            }
        } catch (Exception e) {
            return new CanMatchShardResponse(true, null);
        } finally {
            Releasables.close(releasable);
        }
    }

    /**
     * This method tries to rewrite a query without using a {@link SearchExecutionContext}. It takes advantage of the fact that
     * we can skip some shards in the query phase because we have enough information in the index mapping to decide the 'can match'
     * outcome. One such example is a term based query against a constant keyword field. This queries can rewrite themselves to a
     * {@link MatchNoneQueryBuilder}. This allows us to avoid extra work for example making the shard search active and waiting for
     * refreshes.
     */
    @SuppressWarnings("unchecked")
    public static boolean queryStillMatchesAfterRewrite(ShardSearchRequest request, QueryRewriteContext context) throws IOException {
        Rewriteable.rewrite(request.getRewriteable(), context, false);
        if (request.getAliasFilter().getQueryBuilder() instanceof MatchNoneQueryBuilder) {
            return false;
        }
        final var source = request.source();
        return canRewriteToMatchNone(source) == false
            || source.subSearches().stream().anyMatch(sqwb -> sqwb.getQueryBuilder() instanceof MatchNoneQueryBuilder == false);
    }

    /**
     * Returns true iff the given search source builder can be early terminated by rewriting to a match none query. Or in other words
     * if the execution of the search request can be early terminated without executing it. This is for instance not possible if
     * a global aggregation is part of this request or if there is a suggest builder present.
     */
    public static boolean canRewriteToMatchNone(SearchSourceBuilder source) {
        if (source == null || source.suggest() != null) {
            return false;
        }
        if (source.subSearches().isEmpty()
            || source.subSearches().stream().anyMatch(sqwb -> sqwb.getQueryBuilder() instanceof MatchAllQueryBuilder)) {
            return false;
        }
        AggregatorFactories.Builder aggregations = source.aggregations();
        return aggregations == null || aggregations.mustVisitAllDocs() == false;
    }

    @SuppressWarnings("unchecked")
    private void rewriteAndFetchShardRequest(IndexShard shard, ShardSearchRequest request, ActionListener<ShardSearchRequest> listener) {
        // we also do rewrite on the coordinating node (TransportSearchService) but we also need to do it here.
        // AliasFilters and other things may need to be rewritten on the data node, but not per individual shard.
        // These are uncommon-cases, but we are very efficient doing the rewrite here.
        Rewriteable.rewriteAndFetch(
            request.getRewriteable(),
            indicesService.getDataRewriteContext(request::nowInMillis),
            request.readerId() == null
                ? listener.delegateFailureAndWrap((l, r) -> shard.ensureShardSearchActive(b -> l.onResponse(request)))
                : listener.safeMap(r -> request)
        );
    }

    /**
     * Returns a new {@link QueryRewriteContext} with the given {@code now} provider
     */
    public QueryRewriteContext getRewriteContext(LongSupplier nowInMillis, ResolvedIndices resolvedIndices, PointInTimeBuilder pit) {
        return getRewriteContext(nowInMillis, resolvedIndices, pit, false);
    }

    /**
     * Returns a new {@link QueryRewriteContext} with the given {@code now} provider
     */
    public QueryRewriteContext getRewriteContext(
        LongSupplier nowInMillis,
        ResolvedIndices resolvedIndices,
        PointInTimeBuilder pit,
        final boolean isExplain
    ) {
        return indicesService.getRewriteContext(nowInMillis, resolvedIndices, pit, isExplain);
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
                return new AggregationReduceContext.ForPartial(
                    bigArrays,
                    scriptService,
                    isCanceled,
                    aggs,
                    multiBucketConsumerService.createForPartial()
                );
            }

            @Override
            public AggregationReduceContext forFinalReduction() {
                return new AggregationReduceContext.ForFinal(
                    bigArrays,
                    scriptService,
                    isCanceled,
                    aggs,
                    multiBucketConsumerService.createForFinal()
                );
            }
        };
    }
}
