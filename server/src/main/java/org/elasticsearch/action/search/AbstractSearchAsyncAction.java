/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.TransportSearchAction.SearchTimeProvider;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.transport.Transport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

/**
 * This is an abstract base class that encapsulates the logic to fan out to all shards in provided {@link List<SearchShardIterator>}
 * and collect the results. If a shard request returns a failure this class handles the advance to the next replica of the shard until
 * the shards replica iterator is exhausted. Each shard is referenced by position in the {@link List<SearchShardIterator>} which is later
 * referred to as the {@code shardIndex}.
 * The fan out and collect algorithm is traditionally used as the initial phase which can either be a query execution or collection of
 * distributed frequencies
 */
abstract class AbstractSearchAsyncAction<Result extends SearchPhaseResult> extends SearchPhase {
    protected static final float DEFAULT_INDEX_BOOST = 1.0f;
    private final Logger logger;
    private final NamedWriteableRegistry namedWriteableRegistry;
    protected final SearchTransportService searchTransportService;
    private final Executor executor;
    private final ActionListener<SearchResponse> listener;
    protected final SearchRequest request;

    /**
     * Used by subclasses to resolve node ids to DiscoveryNodes.
     **/
    private final BiFunction<String, String, Transport.Connection> nodeIdToConnection;
    protected final SearchTask task;
    protected final SearchPhaseResults<Result> results;
    private final long clusterStateVersion;
    protected final Map<String, AliasFilter> aliasFilter;
    protected final Map<String, Float> concreteIndexBoosts;
    private final SetOnce<AtomicArray<ShardSearchFailure>> shardFailures = new SetOnce<>();
    private final AtomicBoolean hasShardResponse = new AtomicBoolean(false);
    private final AtomicInteger successfulOps;
    protected final SearchTimeProvider timeProvider;
    private final SearchResponse.Clusters clusters;

    protected final List<SearchShardIterator> shardsIts;
    protected final SearchShardIterator[] shardIterators;
    private final AtomicInteger outstandingShards;
    private final int maxConcurrentRequestsPerNode;
    private final Map<String, PendingExecutions> pendingExecutionsPerNode;
    private final AtomicBoolean requestCancelled = new AtomicBoolean();
    private final int skippedCount;

    // protected for tests
    protected final SubscribableListener<Void> doneFuture = new SubscribableListener<>();

    AbstractSearchAsyncAction(
        String name,
        Logger logger,
        NamedWriteableRegistry namedWriteableRegistry,
        SearchTransportService searchTransportService,
        BiFunction<String, String, Transport.Connection> nodeIdToConnection,
        Map<String, AliasFilter> aliasFilter,
        Map<String, Float> concreteIndexBoosts,
        Executor executor,
        SearchRequest request,
        ActionListener<SearchResponse> listener,
        List<SearchShardIterator> shardsIts,
        SearchTimeProvider timeProvider,
        ClusterState clusterState,
        SearchTask task,
        SearchPhaseResults<Result> resultConsumer,
        int maxConcurrentRequestsPerNode,
        SearchResponse.Clusters clusters
    ) {
        super(name);
        this.namedWriteableRegistry = namedWriteableRegistry;
        final List<SearchShardIterator> iterators = new ArrayList<>();
        int skipped = 0;
        for (final SearchShardIterator iterator : shardsIts) {
            if (iterator.skip()) {
                skipped++;
            } else {
                iterators.add(iterator);
            }
        }
        this.skippedCount = skipped;
        this.shardsIts = iterators;
        outstandingShards = new AtomicInteger(iterators.size());
        successfulOps = new AtomicInteger(skipped);
        this.shardIterators = iterators.toArray(new SearchShardIterator[0]);
        // we later compute the shard index based on the natural order of the shards
        // that participate in the search request. This means that this number is
        // consistent between two requests that target the same shards.
        Arrays.sort(shardIterators);
        this.maxConcurrentRequestsPerNode = maxConcurrentRequestsPerNode;
        // in the case were we have less shards than maxConcurrentRequestsPerNode we don't need to throttle
        this.pendingExecutionsPerNode = maxConcurrentRequestsPerNode < shardsIts.size() ? new ConcurrentHashMap<>() : null;
        this.timeProvider = timeProvider;
        this.logger = logger;
        this.searchTransportService = searchTransportService;
        this.executor = executor;
        this.request = request;
        this.task = task;
        this.listener = ActionListener.runBefore(listener, () -> doneFuture.onResponse(null));
        this.nodeIdToConnection = nodeIdToConnection;
        this.concreteIndexBoosts = concreteIndexBoosts;
        this.clusterStateVersion = clusterState.version();
        this.aliasFilter = aliasFilter;
        this.results = resultConsumer;
        // register the release of the query consumer to free up the circuit breaker memory
        // at the end of the search
        addReleasable(resultConsumer);
        this.clusters = clusters;
    }

    protected void notifyListShards(
        SearchProgressListener progressListener,
        SearchResponse.Clusters clusters,
        SearchRequest searchRequest,
        List<SearchShardIterator> allIterators
    ) {
        final List<SearchShard> skipped = new ArrayList<>(allIterators.size() - shardsIts.size());
        for (SearchShardIterator iter : allIterators) {
            if (iter.skip()) {
                skipped.add(new SearchShard(iter.getClusterAlias(), iter.shardId()));
            }
        }
        var sourceBuilder = searchRequest.source();
        progressListener.notifyListShards(
            SearchProgressListener.buildSearchShardsFromIter(this.shardsIts),
            skipped,
            clusters,
            sourceBuilder == null || sourceBuilder.size() > 0,
            timeProvider
        );
    }

    /**
     * Registers a {@link Releasable} that will be closed when the search request finishes or fails.
     */
    public void addReleasable(Releasable releasable) {
        var doneFuture = this.doneFuture;
        if (doneFuture.isDone()) {
            releasable.close();
        } else {
            doneFuture.addListener(ActionListener.releasing(releasable));
        }
    }

    /**
     * Builds how long it took to execute the search.
     */
    long buildTookInMillis() {
        return timeProvider.buildTookInMillis();
    }

    /**
     * This is the main entry point for a search. This method starts the search execution of the initial phase.
     */
    public final void start() {
        if (getNumShards() == 0) {
            // no search shards to search on, bail with empty response
            // (it happens with search across _all with no indices around and consistent with broadcast operations)
            int trackTotalHitsUpTo = request.source() == null ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO
                : request.source().trackTotalHitsUpTo() == null ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO
                : request.source().trackTotalHitsUpTo();
            // total hits is null in the response if the tracking of total hits is disabled
            boolean withTotalHits = trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_DISABLED;
            sendSearchResponse(
                withTotalHits ? SearchResponseSections.EMPTY_WITH_TOTAL_HITS : SearchResponseSections.EMPTY_WITHOUT_TOTAL_HITS,
                new AtomicArray<>(0)
            );
            return;
        }
        executePhase(this);
    }

    @Override
    protected final void run() {
        if (outstandingShards.get() == 0) {
            onPhaseDone();
            return;
        }
        final Map<SearchShardIterator, Integer> shardIndexMap = Maps.newHashMapWithExpectedSize(shardIterators.length);
        for (int i = 0; i < shardIterators.length; i++) {
            shardIndexMap.put(shardIterators[i], i);
        }
        doRun(shardIndexMap);
    }

    protected void doRun(Map<SearchShardIterator, Integer> shardIndexMap) {
        doCheckNoMissingShards(getName(), request, shardsIts);
        for (int i = 0; i < shardsIts.size(); i++) {
            final SearchShardIterator shardRoutings = shardsIts.get(i);
            assert shardRoutings.skip() == false;
            assert shardIndexMap.containsKey(shardRoutings);
            int shardIndex = shardIndexMap.get(shardRoutings);
            final SearchShardTarget routing = shardRoutings.nextOrNull();
            if (routing == null) {
                failOnUnavailable(shardIndex, shardRoutings);
            } else {
                performPhaseOnShard(shardIndex, shardRoutings, routing);
            }
        }
    }

    protected final void performPhaseOnShard(final int shardIndex, final SearchShardIterator shardIt, final SearchShardTarget shard) {
        var pendingExecutionsPerNode = this.pendingExecutionsPerNode;
        if (pendingExecutionsPerNode != null) {
            var pendingExecutions = pendingExecutionsPerNode.computeIfAbsent(
                shard.getNodeId(),
                n -> new PendingExecutions(maxConcurrentRequestsPerNode)
            );
            pendingExecutions.submit(l -> doPerformPhaseOnShard(shardIndex, shardIt, shard, l));
        } else {
            doPerformPhaseOnShard(shardIndex, shardIt, shard, () -> {});
        }
    }

    private void doPerformPhaseOnShard(int shardIndex, SearchShardIterator shardIt, SearchShardTarget shard, Releasable releasable) {
        var shardListener = new SearchActionListener<Result>(shard, shardIndex) {
            @Override
            public void innerOnResponse(Result result) {
                try {
                    releasable.close();
                    onShardResult(result);
                } catch (Exception exc) {
                    onShardFailure(shardIndex, shard, shardIt, exc);
                }
            }

            @Override
            public void onFailure(Exception e) {
                releasable.close();
                onShardFailure(shardIndex, shard, shardIt, e);
            }
        };
        final Transport.Connection connection;
        try {
            connection = getConnection(shard.getClusterAlias(), shard.getNodeId());
        } catch (Exception e) {
            shardListener.onFailure(e);
            return;
        }
        executePhaseOnShard(shardIt, connection, shardListener);
    }

    protected final void failOnUnavailable(int shardIndex, SearchShardIterator shardIt) {
        SearchShardTarget unassignedShard = new SearchShardTarget(null, shardIt.shardId(), shardIt.getClusterAlias());
        onShardFailure(shardIndex, unassignedShard, shardIt, new NoShardAvailableActionException(shardIt.shardId()));
    }

    /**
     * Sends the request to the actual shard.
     * @param shardIt the shards iterator
     * @param connection to node that the shard is located on
     * @param listener the listener to notify on response
     */
    protected abstract void executePhaseOnShard(
        SearchShardIterator shardIt,
        Transport.Connection connection,
        SearchActionListener<Result> listener
    );

    /**
     * Processes the phase transition from on phase to another. This method handles all errors that happen during the initial run execution
     * of the next phase. If there are no successful operations in the context when this method is executed the search is aborted and
     * a response is returned to the user indicating that all shards have failed.
     */
    protected void executeNextPhase(String currentPhase, Supplier<SearchPhase> nextPhaseSupplier) {
        /* This is the main search phase transition where we move to the next phase. If all shards
         * failed or if there was a failure and partial results are not allowed, then we immediately
         * fail. Otherwise we continue to the next phase.
         */
        ShardOperationFailedException[] shardSearchFailures = buildShardFailures();
        if (shardSearchFailures.length == getNumShards()) {
            shardSearchFailures = ExceptionsHelper.groupBy(shardSearchFailures);
            Throwable cause = shardSearchFailures.length == 0
                ? null
                : ElasticsearchException.guessRootCauses(shardSearchFailures[0].getCause())[0];
            logger.debug(() -> "All shards failed for phase: [" + currentPhase + "]", cause);
            onPhaseFailure(currentPhase, "all shards failed", cause);
        } else {
            Boolean allowPartialResults = request.allowPartialSearchResults();
            assert allowPartialResults != null : "SearchRequest missing setting for allowPartialSearchResults";
            if (allowPartialResults == false && successfulOps.get() != getNumShards()) {
                // check if there are actual failures in the atomic array since
                // successful retries can reset the failures to null
                if (shardSearchFailures.length > 0) {
                    if (logger.isDebugEnabled()) {
                        int numShardFailures = shardSearchFailures.length;
                        shardSearchFailures = ExceptionsHelper.groupBy(shardSearchFailures);
                        Throwable cause = ElasticsearchException.guessRootCauses(shardSearchFailures[0].getCause())[0];
                        logger.debug(() -> format("%s shards failed for phase: [%s]", numShardFailures, currentPhase), cause);
                    }
                    onPhaseFailure(currentPhase, "Partial shards failure", null);
                } else {
                    int discrepancy = getNumShards() - successfulOps.get();
                    assert discrepancy > 0 : "discrepancy: " + discrepancy;
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                            "Partial shards failure (unavailable: {}, successful: {}, skipped: {}, num-shards: {}, phase: {})",
                            discrepancy,
                            successfulOps.get(),
                            skippedCount,
                            getNumShards(),
                            currentPhase
                        );
                    }
                    onPhaseFailure(currentPhase, "Partial shards failure (" + discrepancy + " shards unavailable)", null);
                }
                return;
            }
            var nextPhase = nextPhaseSupplier.get();
            if (logger.isTraceEnabled()) {
                final String resultsFrom = results.getSuccessfulResults()
                    .map(r -> r.getSearchShardTarget().toString())
                    .collect(Collectors.joining(","));
                logger.trace(
                    "[{}] Moving to next phase: [{}], based on results from: {} (cluster state version: {})",
                    currentPhase,
                    nextPhase.getName(),
                    resultsFrom,
                    clusterStateVersion
                );
            }
            executePhase(nextPhase);
        }
    }

    private void executePhase(SearchPhase phase) {
        try {
            phase.run();
        } catch (RuntimeException e) {
            if (logger.isDebugEnabled()) {
                logger.debug(() -> format("Failed to execute [%s] while moving to [%s] phase", request, phase.getName()), e);
            }
            onPhaseFailure(phase.getName(), "", e);
        }
    }

    private ShardSearchFailure[] buildShardFailures() {
        AtomicArray<ShardSearchFailure> shardFailures = this.shardFailures.get();
        if (shardFailures == null) {
            return ShardSearchFailure.EMPTY_ARRAY;
        }
        List<ShardSearchFailure> entries = shardFailures.asList();
        ShardSearchFailure[] failures = new ShardSearchFailure[entries.size()];
        for (int i = 0; i < failures.length; i++) {
            failures[i] = entries.get(i);
        }
        return failures;
    }

    protected final void onShardFailure(final int shardIndex, SearchShardTarget shard, final SearchShardIterator shardIt, Exception e) {
        // we always add the shard failure for a specific shard instance
        // we do make sure to clean it on a successful response from a shard
        onShardFailure(shardIndex, shard, e);
        final SearchShardTarget nextShard = shardIt.nextOrNull();
        final boolean lastShard = nextShard == null;
        logger.debug(() -> format("%s: Failed to execute [%s] lastShard [%s]", shard, request, lastShard), e);
        if (lastShard) {
            if (request.allowPartialSearchResults() == false) {
                if (requestCancelled.compareAndSet(false, true)) {
                    try {
                        searchTransportService.cancelSearchTask(task, "partial results are not allowed and at least one shard has failed");
                    } catch (Exception cancelFailure) {
                        logger.debug("Failed to cancel search request", cancelFailure);
                    }
                }
            }
            onShardGroupFailure(shardIndex, shard, e);
        }
        if (lastShard == false) {
            performPhaseOnShard(shardIndex, shardIt, nextShard);
        } else {
            // count down outstanding shards, we're done with this shard as there's no more copies to try
            finishOneShard();
        }
    }

    private void finishOneShard() {
        final int outstanding = outstandingShards.decrementAndGet();
        assert outstanding >= 0 : "outstanding: " + outstanding;
        if (outstanding == 0) {
            onPhaseDone();
        }
    }

    /**
     * Executed once for every {@link ShardId} that failed on all available shard routing.
     *
     * @param shardIndex the shard index that failed
     * @param shardTarget the last shard target for this failure
     * @param exc the last failure reason
     */
    protected void onShardGroupFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {}

    /**
     * Executed once for every failed shard level request. This method is invoked before the next replica is tried for the given
     * shard target.
     * @param shardIndex the internal index for this shard. Each shard has an index / ordinal assigned that is used to reference
     *                   it's results
     * @param shardTarget the shard target for this failure
     * @param e the failure reason
     */
    void onShardFailure(final int shardIndex, SearchShardTarget shardTarget, Exception e) {
        if (TransportActions.isShardNotAvailableException(e)) {
            // Groups shard not available exceptions under a generic exception that returns a SERVICE_UNAVAILABLE(503)
            // temporary error.
            e = NoShardAvailableActionException.forOnShardFailureWrapper(e.getMessage());
        }
        // we don't aggregate shard on failures due to the internal cancellation,
        // but do keep the header counts right
        if ((requestCancelled.get() && ExceptionsHelper.isTaskCancelledException(e)) == false) {
            AtomicArray<ShardSearchFailure> shardFailures = this.shardFailures.get();
            // lazily create shard failures, so we can early build the empty shard failure list in most cases (no failures)
            if (shardFailures == null) { // this is double checked locking but it's fine since SetOnce uses a volatile read internally
                synchronized (this.shardFailures) {
                    shardFailures = this.shardFailures.get(); // read again otherwise somebody else has created it?
                    if (shardFailures == null) { // still null so we are the first and create a new instance
                        shardFailures = new AtomicArray<>(getNumShards());
                        this.shardFailures.set(shardFailures);
                    }
                }
            }
            ShardSearchFailure failure = shardFailures.get(shardIndex);
            if (failure == null) {
                shardFailures.set(shardIndex, new ShardSearchFailure(e, shardTarget));
            } else {
                // the failure is already present, try and not override it with an exception that is less meaningless
                // for example, getting illegal shard state
                if (TransportActions.isReadOverrideException(e) && (e instanceof SearchContextMissingException == false)) {
                    shardFailures.set(shardIndex, new ShardSearchFailure(e, shardTarget));
                }
            }

            if (results.hasResult(shardIndex)) {
                assert failure == null : "shard failed before but shouldn't: " + failure;
                successfulOps.decrementAndGet(); // if this shard was successful before (initial phase) we have to adjust the counter
            }
        }
    }

    /**
     * Executed once for every successful shard level request.
     * @param result the result returned form the shard
     */
    protected void onShardResult(Result result) {
        assert result.getShardIndex() != -1 : "shard index is not set";
        assert result.getSearchShardTarget() != null : "search shard target must not be null";
        hasShardResponse.set(true);
        if (logger.isTraceEnabled()) {
            logger.trace("got first-phase result from {}", result != null ? result.getSearchShardTarget() : null);
        }
        // clean a previous error on this shard group (note, this code will be serialized on the same shardIndex value level
        // so it's ok concurrency wise to miss potentially the shard failures being created because of another failure
        // in the #addShardFailure, because by definition, it will happen on *another* shardIndex
        AtomicArray<ShardSearchFailure> shardFailures = this.shardFailures.get();
        if (shardFailures != null) {
            shardFailures.set(result.getShardIndex(), null);
        }
        results.consumeResult(result, () -> {
            successfulOps.incrementAndGet();
            finishOneShard();
        });
    }

    /**
     * Returns the total number of shards to the current search across all indices
     */
    public final int getNumShards() {
        return results.getNumShards();
    }

    /**
    * Returns a logger for this context to prevent each individual phase to create their own logger.
    */
    public final Logger getLogger() {
        return logger;
    }

    /**
      * Returns the currently executing search task
    */
    public final SearchTask getTask() {
        return task;
    }

    /**
      * Returns the currently executing search request
      */
    public final SearchRequest getRequest() {
        return request;
    }

    /**
      * Returns the targeted {@link OriginalIndices} for the provided {@code shardIndex}.
      */
    public OriginalIndices getOriginalIndices(int shardIndex) {
        return shardIterators[shardIndex].getOriginalIndices();
    }

    /**
      * Checks if the given context id is part of the point in time of this search (if exists).
      * We should not release search contexts that belong to the point in time during or after searches.
    */
    public boolean isPartOfPointInTime(ShardSearchContextId contextId) {
        final PointInTimeBuilder pointInTimeBuilder = request.pointInTimeBuilder();
        if (pointInTimeBuilder != null) {
            return request.pointInTimeBuilder().getSearchContextId(namedWriteableRegistry).contains(contextId);
        } else {
            return false;
        }
    }

    private SearchResponse buildSearchResponse(
        SearchResponseSections internalSearchResponse,
        ShardSearchFailure[] failures,
        String scrollId,
        BytesReference searchContextId
    ) {
        int numSuccess = successfulOps.get();
        int numFailures = failures.length;
        assert numSuccess + numFailures == getNumShards()
            : "numSuccess(" + numSuccess + ") + numFailures(" + numFailures + ") != totalShards(" + getNumShards() + ")";
        return new SearchResponse(
            internalSearchResponse,
            scrollId,
            getNumShards(),
            numSuccess,
            skippedCount,
            buildTookInMillis(),
            failures,
            clusters,
            searchContextId
        );
    }

    /**
      * Builds and sends the final search response back to the user.
      *
      * @param internalSearchResponse the internal search response
      * @param queryResults           the results of the query phase
      */
    public void sendSearchResponse(SearchResponseSections internalSearchResponse, AtomicArray<SearchPhaseResult> queryResults) {
        ShardSearchFailure[] failures = buildShardFailures();
        Boolean allowPartialResults = request.allowPartialSearchResults();
        assert allowPartialResults != null : "SearchRequest missing setting for allowPartialSearchResults";
        if (allowPartialResults == false && failures.length > 0) {
            raisePhaseFailure(new SearchPhaseExecutionException("", "Shard failures", null, failures));
        } else {
            ActionListener.respondAndRelease(
                listener,
                buildSearchResponse(
                    internalSearchResponse,
                    failures,
                    request.scroll() != null ? TransportSearchHelper.buildScrollId(queryResults) : null,
                    buildSearchContextId(failures)
                )
            );
        }
    }

    protected BytesReference buildSearchContextId(ShardSearchFailure[] failures) {
        var source = request.source();
        return source != null && source.pointInTimeBuilder() != null && source.pointInTimeBuilder().singleSession() == false
            ? source.pointInTimeBuilder().getEncodedId()
            : null;
    }

    /**
     * This method will communicate a fatal phase failure back to the user. In contrast to a shard failure
     * will this method immediately fail the search request and return the failure to the issuer of the request
     * @param phase the phase that failed
     * @param msg an optional message
     * @param cause the cause of the phase failure
     */
    public void onPhaseFailure(String phase, String msg, Throwable cause) {
        raisePhaseFailure(new SearchPhaseExecutionException(phase, msg, cause, buildShardFailures()));
    }

    /**
     * This method should be called if a search phase failed to ensure all relevant reader contexts are released.
     * This method will also notify the listener and sends back a failure to the user.
     *
     * @param exception the exception explaining or causing the phase failure
     */
    private void raisePhaseFailure(SearchPhaseExecutionException exception) {
        results.getSuccessfulResults().forEach((entry) -> {
            // Do not release search contexts that are part of the point in time
            if (entry.getContextId() != null && isPartOfPointInTime(entry.getContextId()) == false) {
                try {
                    SearchShardTarget searchShardTarget = entry.getSearchShardTarget();
                    Transport.Connection connection = getConnection(searchShardTarget.getClusterAlias(), searchShardTarget.getNodeId());
                    sendReleaseSearchContext(entry.getContextId(), connection);
                } catch (Exception inner) {
                    inner.addSuppressed(exception);
                    logger.trace("failed to release context", inner);
                }
            }
        });
        listener.onFailure(exception);
    }

    /**
      * Releases a search context with the given context ID on the node the given connection is connected to.
      * @see org.elasticsearch.search.query.QuerySearchResult#getContextId()
      * @see org.elasticsearch.search.fetch.FetchSearchResult#getContextId()
      *
      */
    void sendReleaseSearchContext(ShardSearchContextId contextId, Transport.Connection connection) {
        assert isPartOfPointInTime(contextId) == false : "Must not release point in time context [" + contextId + "]";
        if (connection != null) {
            searchTransportService.sendFreeContext(connection, contextId, ActionListener.noop());
        }
    }

    /**
     * Executed once all shard results have been received and processed
     * @see #onShardFailure(int, SearchShardTarget, Exception)
     * @see #onShardResult(SearchPhaseResult)
     */
    private void onPhaseDone() {  // as a tribute to @kimchy aka. finishHim()
        executeNextPhase(getName(), this::getNextPhase);
    }

    /**
     * Returns a connection to the node if connected otherwise and {@link org.elasticsearch.transport.ConnectTransportException} will be
     * thrown.
     */
    public final Transport.Connection getConnection(String clusterAlias, String nodeId) {
        return nodeIdToConnection.apply(clusterAlias, nodeId);
    }

    /**
     * Returns the {@link SearchTransportService} to send shard request to other nodes
     */
    public SearchTransportService getSearchTransport() {
        return searchTransportService;
    }

    public final void execute(Runnable command) {
        executor.execute(command);
    }

    /**
     * Builds an request for the initial search phase.
     *
     * @param shardIt the target {@link SearchShardIterator}
     * @param shardIndex the index of the shard that is used in the coordinator node to
     *                   tiebreak results with identical sort values
     */
    protected final ShardSearchRequest buildShardSearchRequest(SearchShardIterator shardIt, int shardIndex) {
        AliasFilter filter = aliasFilter.get(shardIt.shardId().getIndex().getUUID());
        assert filter != null;
        float indexBoost = concreteIndexBoosts.getOrDefault(shardIt.shardId().getIndex().getUUID(), DEFAULT_INDEX_BOOST);
        ShardSearchRequest shardRequest = new ShardSearchRequest(
            shardIt.getOriginalIndices(),
            request,
            shardIt.shardId(),
            shardIndex,
            getNumShards(),
            filter,
            indexBoost,
            timeProvider.absoluteStartMillis(),
            shardIt.getClusterAlias(),
            shardIt.getSearchContextId(),
            shardIt.getSearchContextKeepAlive()
        );
        // if we already received a search result we can inform the shard that it
        // can return a null response if the request rewrites to match none rather
        // than creating an empty response in the search thread pool.
        // Note that, we have to disable this shortcut for queries that create a context (scroll and search context).
        shardRequest.canReturnNullResponseIfMatchNoDocs(hasShardResponse.get() && shardRequest.scroll() == null);
        return shardRequest;
    }

    /**
     * Returns the next phase based on the results of the initial search phase
     */
    protected abstract SearchPhase getNextPhase();

    private static final class PendingExecutions {
        private final Semaphore semaphore;
        private final ConcurrentLinkedQueue<Consumer<Releasable>> queue = new ConcurrentLinkedQueue<>();

        PendingExecutions(int permits) {
            assert permits > 0 : "not enough permits: " + permits;
            semaphore = new Semaphore(permits);
        }

        void submit(Consumer<Releasable> task) {
            if (semaphore.tryAcquire()) {
                executeAndRelease(task);
            } else {
                queue.add(task);
                if (semaphore.tryAcquire()) {
                    task = pollNextTaskOrReleasePermit();
                    if (task != null) {
                        executeAndRelease(task);
                    }
                }
            }
        }

        private void executeAndRelease(Consumer<Releasable> task) {
            do {
                final SubscribableListener<Void> onDone = new SubscribableListener<>();
                task.accept(() -> onDone.onResponse(null));
                if (onDone.isDone()) {
                    // keep going on the current thread, no need to fork
                    task = pollNextTaskOrReleasePermit();
                } else {
                    onDone.addListener(new ActionListener<>() {
                        @Override
                        public void onResponse(Void unused) {
                            final Consumer<Releasable> nextTask = pollNextTaskOrReleasePermit();
                            if (nextTask != null) {
                                executeAndRelease(nextTask);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assert false : e;
                        }
                    });
                    return;
                }
            } while (task != null);
        }

        private Consumer<Releasable> pollNextTaskOrReleasePermit() {
            var task = queue.poll();
            if (task == null) {
                semaphore.release();
                while (queue.peek() != null && semaphore.tryAcquire()) {
                    task = queue.poll();
                    if (task == null) {
                        semaphore.release();
                    } else {
                        return task;
                    }
                }
            }
            return task;
        }
    }
}
