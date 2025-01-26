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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.TransportSearchAction.SearchTimeProvider;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.transport.Transport;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

/**
 * This is an abstract base class that encapsulates the logic to fan out to all shards in provided {@link GroupShardsIterator}
 * and collect the results. If a shard request returns a failure this class handles the advance to the next replica of the shard until
 * the shards replica iterator is exhausted. Each shard is referenced by position in the {@link GroupShardsIterator} which is later
 * referred to as the {@code shardIndex}.
 * The fan out and collect algorithm is traditionally used as the initial phase which can either be a query execution or collection of
 * distributed frequencies
 */
abstract class AbstractSearchAsyncAction<Result extends SearchPhaseResult> extends AsyncSearchContext<Result> {
    static final float DEFAULT_INDEX_BOOST = 1.0f;
    protected final Logger logger;

    private final long clusterStateVersion;
    private final int maxConcurrentRequestsPerNode;
    private final Map<String, PendingExecutions> pendingExecutionsPerNode = new ConcurrentHashMap<>();
    private final boolean throttleConcurrentRequests;

    // protected for tests
    protected final String name;

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
        GroupShardsIterator<SearchShardIterator> shardsIts,
        SearchTimeProvider timeProvider,
        ClusterState clusterState,
        SearchTask task,
        SearchPhaseResults<Result> resultConsumer,
        int maxConcurrentRequestsPerNode,
        SearchResponse.Clusters clusters
    ) {
        super(
            request,
            resultConsumer,
            namedWriteableRegistry,
            listener,
            task,
            searchTransportService,
            executor,
            nodeIdToConnection,
            shardsIts,
            aliasFilter,
            concreteIndexBoosts,
            timeProvider,
            clusterState,
            clusters
        );
        this.name = name;
        this.maxConcurrentRequestsPerNode = maxConcurrentRequestsPerNode;
        // in the case were we have less shards than maxConcurrentRequestsPerNode we don't need to throttle
        this.throttleConcurrentRequests = maxConcurrentRequestsPerNode < shardsIts.size();
        this.logger = logger;
        this.clusterStateVersion = clusterState.version();
    }

    protected String missingShardsErrorMessage(StringBuilder missingShards) {
        return makeMissingShardsError(missingShards);
    }

    protected static String makeMissingShardsError(StringBuilder missingShards) {
        return "Search rejected due to missing shards ["
            + missingShards
            + "]. Consider using `allow_partial_search_results` setting to bypass this error.";
    }

    protected void doCheckNoMissingShards(String phaseName, SearchRequest request, GroupShardsIterator<SearchShardIterator> shardsIts) {
        doCheckNoMissingShards(phaseName, request, shardsIts, this::missingShardsErrorMessage);
    }

    protected static void doCheckNoMissingShards(
        String phaseName,
        SearchRequest request,
        GroupShardsIterator<SearchShardIterator> shardsIts,
        Function<StringBuilder, String> makeErrorMessage
    ) {
        assert request.allowPartialSearchResults() != null : "SearchRequest missing setting for allowPartialSearchResults";
        if (request.allowPartialSearchResults() == false) {
            final StringBuilder missingShards = new StringBuilder();
            // Fail-fast verification of all shards being available
            for (int index = 0; index < shardsIts.size(); index++) {
                final SearchShardIterator shardRoutings = shardsIts.get(index);
                if (shardRoutings.size() == 0) {
                    if (missingShards.isEmpty() == false) {
                        missingShards.append(", ");
                    }
                    missingShards.append(shardRoutings.shardId());
                }
            }
            if (missingShards.isEmpty() == false) {
                // Status red - shard is missing all copies and would produce partial results for an index search
                final String msg = makeErrorMessage.apply(missingShards);
                throw new SearchPhaseExecutionException(phaseName, msg, null, ShardSearchFailure.EMPTY_ARRAY);
            }
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
        if (results.getNumShards() == 0) {
            sendZeroShardsResponse();
            return;
        }
        try {
            run();
        } catch (RuntimeException e) {
            if (logger.isDebugEnabled()) {
                logger.debug(() -> format("Failed to execute [%s] while moving to [%s] phase", request, name), e);
            }
            onPhaseFailure(name, "", e);
        }
    }

    private void run() {
        if (shardsIts.size() == 0) {
            onPhaseDone();
            return;
        }
        final Map<SearchShardIterator, Integer> shardIndexMap = Maps.newHashMapWithExpectedSize(shardIterators.length);
        for (int i = 0; i < shardIterators.length; i++) {
            shardIndexMap.put(shardIterators[i], i);
        }
        if (shardsIts.size() > 0) {
            doCheckNoMissingShards(name, request, shardsIts);
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
    }

    private void performPhaseOnShard(final int shardIndex, final SearchShardIterator shardIt, final SearchShardTarget shard) {
        if (throttleConcurrentRequests) {
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

    private void failOnUnavailable(int shardIndex, SearchShardIterator shardIt) {
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
    public void executeNextPhase(String currentPhase, Supplier<SearchPhase> nextPhaseSupplier) {
        /* This is the main search phase transition where we move to the next phase. If all shards
         * failed or if there was a failure and partial results are not allowed, then we immediately
         * fail. Otherwise we continue to the next phase.
         */
        ShardOperationFailedException[] shardSearchFailures = buildShardFailures(shardFailures);
        final int numShards = results.getNumShards();
        if (shardSearchFailures.length == numShards) {
            shardSearchFailures = ExceptionsHelper.groupBy(shardSearchFailures);
            Throwable cause = shardSearchFailures.length == 0
                ? null
                : ElasticsearchException.guessRootCauses(shardSearchFailures[0].getCause())[0];
            logger.debug(() -> "All shards failed for phase: [" + currentPhase + "]", cause);
            onPhaseFailure(currentPhase, "all shards failed", cause);
        } else {
            Boolean allowPartialResults = request.allowPartialSearchResults();
            assert allowPartialResults != null : "SearchRequest missing setting for allowPartialSearchResults";
            if (allowPartialResults == false && successfulOps.get() != numShards) {
                handleNotAllSucceeded(currentPhase, shardSearchFailures, numShards);
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

    private void onShardFailure(final int shardIndex, SearchShardTarget shard, final SearchShardIterator shardIt, Exception e) {
        // we always add the shard failure for a specific shard instance
        // we do make sure to clean it on a successful response from a shard
        onShardFailure(shardIndex, shard, e);
        final SearchShardTarget nextShard = shardIt.nextOrNull();
        final boolean lastShard = nextShard == null;
        logger.debug(() -> format("%s: Failed to execute [%s] lastShard [%s]", shard, request, lastShard), e);
        if (lastShard) {
            maybeCancelSearchTask();
            onShardGroupFailure(shardIndex, shard, e);
        }
        if (lastShard == false) {
            performPhaseOnShard(shardIndex, shardIt, nextShard);
        } else {
            finishShardAndMaybePhase();
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
    @Override
    public void onShardFailure(final int shardIndex, SearchShardTarget shardTarget, Exception e) {
        if (TransportActions.isShardNotAvailableException(e)) {
            // Groups shard not available exceptions under a generic exception that returns a SERVICE_UNAVAILABLE(503)
            // temporary error.
            e = NoShardAvailableActionException.forOnShardFailureWrapper(e.getMessage());
        }

        handleFailedAndCancelled(shardIndex, shardTarget, e);
    }

    /**
     * Executed once for every successful shard level request.
     * @param result the result returned form the shard
     */
    protected void onShardResult(Result result) {
        assert result.getShardIndex() != -1 : "shard index is not set";
        assert result.getSearchShardTarget() != null : "search shard target must not be null";
        if (hasShardResponse == false) {
            hasShardResponse = true;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("got first-phase result from {}", result.getSearchShardTarget());
        }
        results.consumeResult(result, () -> onShardResultConsumed(result));
    }

    private void onShardResultConsumed(Result result) {
        successfulOps.incrementAndGet();
        // clean a previous error on this shard group (note, this code will be serialized on the same shardIndex value level
        // so its ok concurrency wise to miss potentially the shard failures being created because of another failure
        // in the #addShardFailure, because by definition, it will happen on *another* shardIndex
        AtomicArray<ShardSearchFailure> shardFailures = this.shardFailures.get();
        if (shardFailures != null) {
            shardFailures.set(result.getShardIndex(), null);
        }
        finishShardAndMaybePhase();
    }

    private void finishShardAndMaybePhase() {
        if (finishShard()) {
            onPhaseDone();
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
        final int numShards = results.getNumShards();
        assert numSuccess + numFailures == numShards
            : "numSuccess(" + numSuccess + ") + numFailures(" + numFailures + ") != totalShards(" + numShards + ")";
        return new SearchResponse(
            internalSearchResponse,
            scrollId,
            numShards,
            numSuccess,
            toSkipShardsIts.size(),
            buildTookInMillis(),
            failures,
            clusters,
            searchContextId
        );
    }

    boolean buildPointInTimeFromSearchResults() {
        return false;
    }

    /**
      * Builds and sends the final search response back to the user.
      *
      * @param internalSearchResponse the internal search response
      * @param queryResults           the results of the query phase
      */
    public void sendSearchResponse(SearchResponseSections internalSearchResponse, AtomicArray<? extends SearchPhaseResult> queryResults) {
        ShardSearchFailure[] failures = buildShardFailures(shardFailures);
        Boolean allowPartialResults = request.allowPartialSearchResults();
        assert allowPartialResults != null : "SearchRequest missing setting for allowPartialSearchResults";
        if (allowPartialResults == false && failures.length > 0) {
            raisePhaseFailure(new SearchPhaseExecutionException("", "Shard failures", null, failures));
        } else {
            final String scrollId = request.scroll() != null ? TransportSearchHelper.buildScrollId(queryResults) : null;
            final BytesReference searchContextId;
            if (buildPointInTimeFromSearchResults()) {
                searchContextId = SearchContextId.encode(queryResults.asList(), aliasFilter, minTransportVersion, failures);
            } else {
                searchContextId = buildSearchContextId();
            }
            ActionListener.respondAndRelease(listener, buildSearchResponse(internalSearchResponse, failures, scrollId, searchContextId));
        }
    }

    /**
     * This method will communicate a fatal phase failure back to the user. In contrast to a shard failure
     * will this method immediately fail the search request and return the failure to the issuer of the request
     * @param phase the phase that failed
     * @param msg an optional message
     * @param cause the cause of the phase failure
     */
    @Override
    public void onPhaseFailure(String phase, String msg, Throwable cause) {
        raisePhaseFailure(new SearchPhaseExecutionException(phase, msg, cause, buildShardFailures(shardFailures)));
    }

    /**
      * Releases a search context with the given context ID on the node the given connection is connected to.
      * @see org.elasticsearch.search.query.QuerySearchResult#getContextId()
      * @see org.elasticsearch.search.fetch.FetchSearchResult#getContextId()
      *
      */
    @Override
    public void sendReleaseSearchContext(ShardSearchContextId contextId, Transport.Connection connection) {
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
        executeNextPhase(name, this::getNextPhase);
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
            results.getNumShards(),
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
        shardRequest.canReturnNullResponseIfMatchNoDocs(hasShardResponse && shardRequest.scroll() == null);
        return shardRequest;
    }

    /**
     * Returns the next phase based on the results of the initial search phase
     */
    protected abstract SearchPhase getNextPhase();

    static final class PendingExecutions {
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
