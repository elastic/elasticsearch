/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.Transport;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class DefaultSearchPhaseContext implements SearchPhaseContext {
    private static final float DEFAULT_INDEX_BOOST = 1.0f;

    private final SearchRequest request;
    private final Logger logger;
    private final ActionListener<SearchResponse> listener;
    private final SearchTask task;
    private final Executor executor;
    private final SearchTransportService searchTransportService;
    private final BiFunction<String, String, Transport.Connection> nodeIdToConnection;

    // TODO(jtibs): we shouldn't hold the results both here and in AbstractSearchAsyncAction
    private final SearchPhaseResults<?> results;

    private final Map<String, AliasFilter> aliasFilter;
    private final Map<String, Float> concreteIndexBoosts;
    private final ClusterState clusterState;
    private final SearchResponse.Clusters clusters;
    private final TransportSearchAction.SearchTimeProvider timeProvider;
    private final boolean buildPointInTimeFromSearchResults;

    private final SetOnce<AtomicArray<ShardSearchFailure>> shardFailures = new SetOnce<>();
    private final Object shardFailuresMutex = new Object();
    private final AtomicBoolean hasShardResponse = new AtomicBoolean(false);
    private final AtomicBoolean requestCancelled = new AtomicBoolean();
    private final AtomicInteger successfulOps = new AtomicInteger();
    private final AtomicInteger skippedOps = new AtomicInteger();
    private final List<Releasable> releasables = new ArrayList<>();

    // TODO(jtibs): remove constructor and fix tests
    DefaultSearchPhaseContext(SearchRequest request,
                              SearchTask task,
                              Executor executor,
                              SearchTransportService searchTransportService,
                              BiFunction<String, String, Transport.Connection> nodeIdToConnection) {
        this(request, null, null, null, null, null, task, executor, searchTransportService, nodeIdToConnection, null, null, null, true);
    }

    DefaultSearchPhaseContext(SearchRequest request,
                              Logger logger,
                              ActionListener<SearchResponse> listener,
                              SearchPhaseResults<?> results,
                              Map<String, AliasFilter> aliasFilter,
                              Map<String, Float> concreteIndexBoosts,
                              SearchTask task,
                              Executor executor,
                              SearchTransportService searchTransportService,
                              BiFunction<String, String, Transport.Connection> nodeIdToConnection,
                              ClusterState clusterState,
                              SearchResponse.Clusters clusters,
                              TransportSearchAction.SearchTimeProvider timeProvider,
                              boolean buildPointInTimeFromSearchResults) {
        this.request = request;
        this.logger = logger;
        this.listener = ActionListener.runAfter(listener, () -> Releasables.close(releasables));
        this.results = results;
        this.aliasFilter = aliasFilter;
        this.concreteIndexBoosts = concreteIndexBoosts;
        this.task = task;
        this.executor = executor;
        this.searchTransportService = searchTransportService;
        this.nodeIdToConnection = nodeIdToConnection;
        this.clusterState = clusterState;
        this.clusters = clusters;
        this.timeProvider = timeProvider;
        this.buildPointInTimeFromSearchResults = buildPointInTimeFromSearchResults;
    }

    @Override
    public final int getNumShards() {
        return results.getNumShards();
    }

    @Override
    public final Logger getLogger() {
        return logger;
    }

    @Override
    public final SearchTask getTask() {
        return task;
    }

    @Override
    public final SearchRequest getRequest() {
        return request;
    }

    // TODO(jtibs): introduce better abstractions to avoid exposing all these internal components
    @Override
    public ActionListener<SearchResponse> getListener() {
        return listener;
    }

    @Override
    public SearchResponse.Clusters getClusters() {
        return clusters;
    }

    @Override
    public long buildTookInMillis() {
        return timeProvider.buildTookInMillis();
    }

    @Override
    public AtomicBoolean getRequestCancelled() {
        return requestCancelled;
    }

    @Override
    public AtomicBoolean hasShardResponse() {
        return hasShardResponse;
    }

    @Override
    public AtomicInteger getSuccessfulOps() {
        return successfulOps;
    }

    @Override
    public AtomicInteger getSkippedOps() {
        return skippedOps;
    }

    @Override
    public SetOnce<AtomicArray<ShardSearchFailure>> getShardFailures() {
        return shardFailures;
    }

    @Override
    public boolean isPartOfPointInTime(ShardSearchContextId contextId) {
        final PointInTimeBuilder pointInTimeBuilder = request.pointInTimeBuilder();
        if (pointInTimeBuilder != null) {
            return request.pointInTimeBuilder().getSearchContextId(searchTransportService.getNamedWriteableRegistry()).contains(contextId);
        } else {
            return false;
        }
    }

    @Override
    public final Transport.Connection getConnection(String clusterAlias, String nodeId) {
        Transport.Connection conn = nodeIdToConnection.apply(clusterAlias, nodeId);
        Version minVersion = request.minCompatibleShardNode();
        if (minVersion != null && conn != null && conn.getVersion().before(minVersion)) {
            throw new VersionMismatchException("One of the shards is incompatible with the required minimum version [{}]", minVersion);
        }
        return conn;
    }

    @Override
    public final SearchTransportService getSearchTransport() {
        return searchTransportService;
    }

    @Override
    public final void execute(Runnable command) {
        executor.execute(command);
    }

    @Override
    public void addReleasable(Releasable releasable) {
        releasables.add(releasable);
    }

    @Override
    public void sendSearchResponse(InternalSearchResponse internalSearchResponse, AtomicArray<SearchPhaseResult> queryResults) {
        ShardSearchFailure[] failures = buildShardFailures();
        Boolean allowPartialResults = request.allowPartialSearchResults();
        assert allowPartialResults != null : "SearchRequest missing setting for allowPartialSearchResults";
        if (allowPartialResults == false && failures.length > 0) {
            raisePhaseFailure(new SearchPhaseExecutionException("", "Shard failures", null, failures));
        } else {
            final Version minNodeVersion = clusterState.nodes().getMinNodeVersion();
            final String scrollId = request.scroll() != null ? TransportSearchHelper.buildScrollId(queryResults) : null;
            final String searchContextId;
            if (buildPointInTimeFromSearchResults) {
                searchContextId = SearchContextId.encode(queryResults.asList(), aliasFilter, minNodeVersion);
            } else {
                if (request.source() != null && request.source().pointInTimeBuilder() != null) {
                    searchContextId = request.source().pointInTimeBuilder().getEncodedId();
                } else {
                    searchContextId = null;
                }
            }
            listener.onResponse(buildSearchResponse(internalSearchResponse, failures, scrollId, searchContextId));
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

    private SearchResponse buildSearchResponse(InternalSearchResponse internalSearchResponse, ShardSearchFailure[] failures,
                                               String scrollId, String searchContextId) {
        int numSuccess = successfulOps.get();
        int numFailures = failures.length;
        assert numSuccess + numFailures == getNumShards()
            : "numSuccess(" + numSuccess + ") + numFailures(" + numFailures + ") != totalShards(" + getNumShards() + ")";
        return new SearchResponse(internalSearchResponse, scrollId, getNumShards(), numSuccess,
            skippedOps.get(), buildTookInMillis(), failures, clusters, searchContextId);
    }

    @Override
    public final void onFailure(Exception e) {
        listener.onFailure(e);
    }

    @Override
    public final void onPhaseFailure(SearchPhase phase, String msg, Throwable cause) {
        raisePhaseFailure(new SearchPhaseExecutionException(phase.getName(), msg, cause, buildShardFailures()));
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
                    sendReleaseSearchContext(entry.getContextId(), connection, searchShardTarget.getOriginalIndices());
                } catch (Exception inner) {
                    inner.addSuppressed(exception);
                    logger.trace("failed to release context", inner);
                }
            }
        });
        listener.onFailure(exception);
    }

    @Override
    public final void onShardFailure(final int shardIndex, SearchShardTarget shardTarget, Exception e) {
        if (TransportActions.isShardNotAvailableException(e)) {
            // Groups shard not available exceptions under a generic exception that returns a SERVICE_UNAVAILABLE(503)
            // temporary error.
            e  = new NoShardAvailableActionException(shardTarget.getShardId(), e.getMessage());
        }
        // we don't aggregate shard on failures due to the internal cancellation,
        // but do keep the header counts right
        if ((requestCancelled.get() && isTaskCancelledException(e)) == false) {
            AtomicArray<ShardSearchFailure> shardFailures = this.shardFailures.get();
            // lazily create shard failures, so we can early build the empty shard failure list in most cases (no failures)
            if (shardFailures == null) { // this is double checked locking but it's fine since SetOnce uses a volatile read internally
                synchronized (shardFailuresMutex) {
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
        results.consumeShardFailure(shardIndex);
    }

    private static boolean isTaskCancelledException(Exception e) {
        return ExceptionsHelper.unwrapCausesAndSuppressed(e, ex -> ex instanceof TaskCancelledException).isPresent();
    }

    @Override
    public final ShardSearchRequest buildShardSearchRequest(SearchShardIterator shardIt, int shardIndex) {
        AliasFilter filter = aliasFilter.get(shardIt.shardId().getIndex().getUUID());
        assert filter != null;
        float indexBoost = concreteIndexBoosts.getOrDefault(shardIt.shardId().getIndex().getUUID(), DEFAULT_INDEX_BOOST);
        ShardSearchRequest shardRequest = new ShardSearchRequest(shardIt.getOriginalIndices(), request, shardIt.shardId(), shardIndex,
            getNumShards(), filter, indexBoost, timeProvider.getAbsoluteStartMillis(), shardIt.getClusterAlias(),
            shardIt.getSearchContextId(), shardIt.getSearchContextKeepAlive());
        // if we already received a search result we can inform the shard that it
        // can return a null response if the request rewrites to match none rather
        // than creating an empty response in the search thread pool.
        // Note that, we have to disable this shortcut for queries that create a context (scroll and search context).
        shardRequest.canReturnNullResponseIfMatchNoDocs(hasShardResponse.get() && shardRequest.scroll() == null);
        return shardRequest;
    }

    @Override
    public final void executeNextPhase(SearchPhase currentPhase, SearchPhase nextPhase) {
        /* This is the main search phase transition where we move to the next phase. If all shards
         * failed or if there was a failure and partial results are not allowed, then we immediately
         * fail. Otherwise we continue to the next phase.
         */
        ShardOperationFailedException[] shardSearchFailures = buildShardFailures();
        if (shardSearchFailures.length == getNumShards()) {
            shardSearchFailures = ExceptionsHelper.groupBy(shardSearchFailures);
            Throwable cause = shardSearchFailures.length == 0 ? null :
                ElasticsearchException.guessRootCauses(shardSearchFailures[0].getCause())[0];
            logger.debug(() -> new ParameterizedMessage("All shards failed for phase: [{}]", currentPhase.getName()), cause);
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
                        logger.debug(() -> new ParameterizedMessage("{} shards failed for phase: [{}]",
                            numShardFailures, currentPhase.getName()), cause);
                    }
                    onPhaseFailure(currentPhase, "Partial shards failure", null);
                    return;
                } else {
                    int discrepancy = getNumShards() - successfulOps.get();
                    assert discrepancy > 0 : "discrepancy: " + discrepancy;
                    if (logger.isDebugEnabled()) {
                        logger.debug("Partial shards failure (unavailable: {}, successful: {}, skipped: {}, num-shards: {}, phase: {})",
                            discrepancy, successfulOps.get(), skippedOps.get(), getNumShards(), currentPhase.getName());
                    }
                    onPhaseFailure(currentPhase, "Partial shards failure (" + discrepancy + " shards unavailable)", null);
                    return;
                }
            }
            if (logger.isTraceEnabled()) {
                final String resultsFrom = results.getSuccessfulResults()
                    .map(r -> r.getSearchShardTarget().toString()).collect(Collectors.joining(","));
                logger.trace("[{}] Moving to next phase: [{}], based on results from: {} (cluster state version: {})",
                    currentPhase.getName(), nextPhase.getName(), resultsFrom, clusterState.version());
            }
            executePhase(nextPhase);
        }
    }


    private void executePhase(SearchPhase phase) {
        try {
            phase.run();
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug(new ParameterizedMessage("Failed to execute [{}] while moving to [{}] phase", request, phase.getName()), e);
            }
            onPhaseFailure(phase, "", e);
        }
    }

}
