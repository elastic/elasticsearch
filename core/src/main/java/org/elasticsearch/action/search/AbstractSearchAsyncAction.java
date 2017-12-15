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

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.transport.Transport;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

abstract class AbstractSearchAsyncAction<Result extends SearchPhaseResult> extends InitialSearchPhase<Result>
    implements SearchPhaseContext {
    private static final float DEFAULT_INDEX_BOOST = 1.0f;
    private final Logger logger;
    private final SearchTransportService searchTransportService;
    private final Executor executor;
    private final ActionListener<SearchResponse> listener;
    private final SearchRequest request;
    /**
     * Used by subclasses to resolve node ids to DiscoveryNodes.
     **/
    private final BiFunction<String, String, Transport.Connection> nodeIdToConnection;
    private final SearchTask task;
    private final SearchPhaseResults<Result> results;
    private final long clusterStateVersion;
    private final Map<String, AliasFilter> aliasFilter;
    private final Map<String, Float> concreteIndexBoosts;
    private final SetOnce<AtomicArray<ShardSearchFailure>> shardFailures = new SetOnce<>();
    private final Object shardFailuresMutex = new Object();
    private final AtomicInteger successfulOps = new AtomicInteger();
    private final AtomicInteger skippedOps = new AtomicInteger();
    private final TransportSearchAction.SearchTimeProvider timeProvider;
    private final SearchResponse.Clusters clusters;

    protected AbstractSearchAsyncAction(String name, Logger logger, SearchTransportService searchTransportService,
                                        BiFunction<String, String, Transport.Connection> nodeIdToConnection,
                                        Map<String, AliasFilter> aliasFilter, Map<String, Float> concreteIndexBoosts,
                                        Executor executor, SearchRequest request,
                                        ActionListener<SearchResponse> listener, GroupShardsIterator<SearchShardIterator> shardsIts,
                                        TransportSearchAction.SearchTimeProvider timeProvider, long clusterStateVersion,
                                        SearchTask task, SearchPhaseResults<Result> resultConsumer, int maxConcurrentShardRequests,
                                        SearchResponse.Clusters clusters) {
        super(name, request, shardsIts, logger, maxConcurrentShardRequests, executor);
        this.timeProvider = timeProvider;
        this.logger = logger;
        this.searchTransportService = searchTransportService;
        this.executor = executor;
        this.request = request;
        this.task = task;
        this.listener = listener;
        this.nodeIdToConnection = nodeIdToConnection;
        this.clusterStateVersion = clusterStateVersion;
        this.concreteIndexBoosts = concreteIndexBoosts;
        this.aliasFilter = aliasFilter;
        this.results = resultConsumer;
        this.clusters = clusters;
    }

    /**
     * Builds how long it took to execute the search.
     */
    long buildTookInMillis() {
        return TimeUnit.NANOSECONDS.toMillis(
                timeProvider.getRelativeCurrentNanos() - timeProvider.getRelativeStartNanos());
    }

    /**
     * This is the main entry point for a search. This method starts the search execution of the initial phase.
     */
    public final void start() {
        if (getNumShards() == 0) {
            //no search shards to search on, bail with empty response
            //(it happens with search across _all with no indices around and consistent with broadcast operations)
            listener.onResponse(new SearchResponse(InternalSearchResponse.empty(), null, 0, 0, 0, buildTookInMillis(),
                ShardSearchFailure.EMPTY_ARRAY, clusters));
            return;
        }
        executePhase(this);
    }

    @Override
    public final void executeNextPhase(SearchPhase currentPhase, SearchPhase nextPhase) {
        /* This is the main search phase transition where we move to the next phase. At this point we check if there is
         * at least one successful operation left and if so we move to the next phase. If not we immediately fail the
         * search phase as "all shards failed"*/
        if (successfulOps.get() == 0) { // we have 0 successful results that means we shortcut stuff and return a failure
            if (logger.isDebugEnabled()) {
                final ShardOperationFailedException[] shardSearchFailures = ExceptionsHelper.groupBy(buildShardFailures());
                Throwable cause = shardSearchFailures.length == 0 ? null :
                    ElasticsearchException.guessRootCauses(shardSearchFailures[0].getCause())[0];
                logger.debug((Supplier<?>) () -> new ParameterizedMessage("All shards failed for phase: [{}]", getName()),
                    cause);
            }
            onPhaseFailure(currentPhase, "all shards failed", null);
        } else {
            if (logger.isTraceEnabled()) {
                final String resultsFrom = results.getSuccessfulResults()
                    .map(r -> r.getSearchShardTarget().toString()).collect(Collectors.joining(","));
                logger.trace("[{}] Moving to next phase: [{}], based on results from: {} (cluster state version: {})",
                    currentPhase.getName(), nextPhase.getName(), resultsFrom, clusterStateVersion);
            }
            executePhase(nextPhase);
        }
    }

    private void executePhase(SearchPhase phase) {
        try {
            phase.run();
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "Failed to execute [{}] while moving to [{}] phase", request, phase.getName()),
                    e);
            }
            onPhaseFailure(phase, "", e);
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

    public final void onShardFailure(final int shardIndex, @Nullable SearchShardTarget shardTarget, Exception e) {
        // we don't aggregate shard failures on non active shards (but do keep the header counts right)
        if (TransportActions.isShardNotAvailableException(e) == false) {
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
                if (TransportActions.isReadOverrideException(e)) {
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

    /**
     * This method should be called if a search phase failed to ensure all relevant search contexts and resources are released.
     * this method will also notify the listener and sends back a failure to the user.
     *
     * @param exception the exception explaining or causing the phase failure
     */
    private void raisePhaseFailure(SearchPhaseExecutionException exception) {
        results.getSuccessfulResults().forEach((entry) -> {
            try {
                SearchShardTarget searchShardTarget = entry.getSearchShardTarget();
                Transport.Connection connection = getConnection(null, searchShardTarget.getNodeId());
                sendReleaseSearchContext(entry.getRequestId(), connection, searchShardTarget.getOriginalIndices());
            } catch (Exception inner) {
                inner.addSuppressed(exception);
                logger.trace("failed to release context", inner);
            }
        });
        listener.onFailure(exception);
    }

    @Override
    public final void onShardSuccess(Result result) {
        successfulOps.incrementAndGet();
        results.consumeResult(result);
        if (logger.isTraceEnabled()) {
            logger.trace("got first-phase result from {}", result != null ? result.getSearchShardTarget() : null);
        }
        // clean a previous error on this shard group (note, this code will be serialized on the same shardIndex value level
        // so its ok concurrency wise to miss potentially the shard failures being created because of another failure
        // in the #addShardFailure, because by definition, it will happen on *another* shardIndex
        AtomicArray<ShardSearchFailure> shardFailures = this.shardFailures.get();
        if (shardFailures != null) {
            shardFailures.set(result.getShardIndex(), null);
        }
    }

    @Override
    public final void onPhaseDone() {
        executeNextPhase(this, getNextPhase(results, this));
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

    @Override
    public final SearchResponse buildSearchResponse(InternalSearchResponse internalSearchResponse, String scrollId) {
        return new SearchResponse(internalSearchResponse, scrollId, getNumShards(), successfulOps.get(),
            skippedOps.get(), buildTookInMillis(), buildShardFailures(), clusters);
    }

    @Override
    public final void onPhaseFailure(SearchPhase phase, String msg, Throwable cause) {
        raisePhaseFailure(new SearchPhaseExecutionException(phase.getName(), msg, cause, buildShardFailures()));
    }

    @Override
    public final Transport.Connection getConnection(String clusterAlias, String nodeId) {
        return nodeIdToConnection.apply(clusterAlias, nodeId);
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
    public final void onResponse(SearchResponse response) {
        listener.onResponse(response);
    }

    @Override
    public final void onFailure(Exception e) {
        listener.onFailure(e);
    }

    public final ShardSearchTransportRequest buildShardSearchRequest(SearchShardIterator shardIt) {
        String clusterAlias = shardIt.getClusterAlias();
        AliasFilter filter = aliasFilter.get(shardIt.shardId().getIndex().getUUID());
        assert filter != null;
        float indexBoost = concreteIndexBoosts.getOrDefault(shardIt.shardId().getIndex().getUUID(), DEFAULT_INDEX_BOOST);
        return new ShardSearchTransportRequest(shardIt.getOriginalIndices(), request, shardIt.shardId(), getNumShards(),
            filter, indexBoost, timeProvider.getAbsoluteStartMillis(), clusterAlias);
    }

    /**
     * Returns the next phase based on the results of the initial search phase
     * @param results the results of the initial search phase. Each non null element in the result array represent a successfully
     *                executed shard request
     * @param context the search context for the next phase
     */
    protected abstract SearchPhase getNextPhase(SearchPhaseResults<Result> results, SearchPhaseContext context);

    @Override
    protected void skipShard(SearchShardIterator iterator) {
        successfulOps.incrementAndGet();
        skippedOps.incrementAndGet();
        super.skipShard(iterator);
    }
}
