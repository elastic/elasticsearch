/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.transport.Transport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;

public abstract class AsyncSearchContext<Result extends SearchPhaseResult> {

    private static final Logger logger = LogManager.getLogger(AsyncSearchContext.class);

    protected final SearchRequest request;

    protected final SearchPhaseResults<Result> results;

    private final NamedWriteableRegistry namedWriteableRegistry;

    protected final ActionListener<SearchResponse> listener;

    protected volatile boolean hasShardResponse = false;

    // protected for tests
    protected final List<Releasable> releasables = new ArrayList<>();

    protected final AtomicBoolean requestCancelled = new AtomicBoolean();

    protected final SearchTask task;

    protected final AtomicInteger successfulOps = new AtomicInteger();

    protected final SearchTransportService searchTransportService;
    protected final Executor executor;

    protected final GroupShardsIterator<SearchShardIterator> toSkipShardsIts;
    protected final GroupShardsIterator<SearchShardIterator> shardsIts;
    protected final SearchShardIterator[] shardIterators;

    /**
     * Used by subclasses to resolve node ids to DiscoveryNodes.
     **/
    protected final BiFunction<String, String, Transport.Connection> nodeIdToConnection;

    protected AsyncSearchContext(
        SearchRequest request,
        SearchPhaseResults<Result> results,
        NamedWriteableRegistry namedWriteableRegistry,
        ActionListener<SearchResponse> listener,
        SearchTask task,
        SearchTransportService searchTransportService,
        Executor executor,
        BiFunction<String, String, Transport.Connection> nodeIdToConnection,
        GroupShardsIterator<SearchShardIterator> shardsIts
    ) {
        final List<SearchShardIterator> toSkipIterators = new ArrayList<>();
        final List<SearchShardIterator> iterators = new ArrayList<>();
        for (final SearchShardIterator iterator : shardsIts) {
            if (iterator.skip()) {
                toSkipIterators.add(iterator);
            } else {
                iterators.add(iterator);
            }
        }
        this.toSkipShardsIts = new GroupShardsIterator<>(toSkipIterators);
        this.successfulOps.setRelease(toSkipIterators.size());
        this.shardsIts = new GroupShardsIterator<>(iterators);

        this.shardIterators = iterators.toArray(new SearchShardIterator[0]);
        // we later compute the shard index based on the natural order of the shards
        // that participate in the search request. This means that this number is
        // consistent between two requests that target the same shards.
        Arrays.sort(shardIterators);
        this.request = request;
        this.results = results;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.listener = ActionListener.runAfter(listener, () -> Releasables.close(releasables));
        this.task = task;
        this.searchTransportService = searchTransportService;
        this.executor = executor;
        this.nodeIdToConnection = nodeIdToConnection;
        // register the release of the query consumer to free up the circuit breaker memory
        // at the end of the search
        addReleasable(results);
    }

    static ShardSearchFailure[] buildShardFailures(SetOnce<AtomicArray<ShardSearchFailure>> shardFailuresRef) {
        AtomicArray<ShardSearchFailure> shardFailures = shardFailuresRef.get();
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

    static boolean isPartOfPIT(NamedWriteableRegistry namedWriteableRegistry, SearchRequest request, ShardSearchContextId contextId) {
        final PointInTimeBuilder pointInTimeBuilder = request.pointInTimeBuilder();
        if (pointInTimeBuilder != null) {
            return request.pointInTimeBuilder().getSearchContextId(namedWriteableRegistry).contains(contextId);
        } else {
            return false;
        }
    }

    /**
     * Returns the currently executing search request
     */
    public final SearchRequest getRequest() {
        return request;
    }

    abstract void sendSearchResponse(SearchResponseSections internalSearchResponse, AtomicArray<? extends SearchPhaseResult> queryResults);

    /**
     * Returns the {@link SearchTransportService} to send shard request to other nodes
     */
    public SearchTransportService getSearchTransport() {
        return searchTransportService;
    }

    /**
     * Returns the currently executing search task
     */
    public final SearchTask getTask() {
        return task;
    }

    abstract void onPhaseFailure(String phase, String msg, Throwable cause);

    /**
     * Registers a {@link Releasable} that will be closed when the search request finishes or fails.
     */
    public final void addReleasable(Releasable releasable) {
        releasables.add(releasable);
    }

    abstract void execute(Runnable command);

    abstract void onShardFailure(int shardIndex, SearchShardTarget shard, Exception e);

    public final Transport.Connection getConnection(String clusterAlias, String nodeId) {
        return nodeIdToConnection.apply(clusterAlias, nodeId);
    }

    abstract OriginalIndices getOriginalIndices(int shardIndex);

    abstract void sendReleaseSearchContext(ShardSearchContextId contextId, Transport.Connection connection);

    abstract void executeNextPhase(String currentPhase, Supplier<SearchPhase> nextPhaseSupplier);

    /**
     * This method should be called if a search phase failed to ensure all relevant reader contexts are released.
     * This method will also notify the listener and sends back a failure to the user.
     *
     * @param exception the exception explaining or causing the phase failure
     */
    protected final void raisePhaseFailure(SearchPhaseExecutionException exception) {
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
     * Checks if the given context id is part of the point in time of this search (if exists).
     * We should not release search contexts that belong to the point in time during or after searches.
     */
    public boolean isPartOfPointInTime(ShardSearchContextId contextId) {
        return isPartOfPIT(namedWriteableRegistry, request, contextId);
    }

    protected final void executePhase(SearchPhase phase) {
        try {
            phase.run();
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug(() -> format("Failed to execute [%s] while moving to [%s] phase", request, phase.getName()), e);
            }
            onPhaseFailure(phase.getName(), "", e);
        }
    }

    protected final void handleNotAllSucceeded(String currentPhase, ShardOperationFailedException[] shardSearchFailures, int numShards) {
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
            int discrepancy = numShards - successfulOps.get();
            assert discrepancy > 0 : "discrepancy: " + discrepancy;
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "Partial shards failure (unavailable: {}, successful: {}, skipped: {}, num-shards: {}, phase: {})",
                    discrepancy,
                    successfulOps.get(),
                    toSkipShardsIts.size(),
                    numShards,
                    currentPhase
                );
            }
            onPhaseFailure(currentPhase, "Partial shards failure (" + discrepancy + " shards unavailable)", null);
        }
    }

}
