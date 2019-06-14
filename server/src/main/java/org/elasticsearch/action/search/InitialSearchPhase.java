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
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * This is an abstract base class that encapsulates the logic to fan out to all shards in provided {@link GroupShardsIterator}
 * and collect the results. If a shard request returns a failure this class handles the advance to the next replica of the shard until
 * the shards replica iterator is exhausted. Each shard is referenced by position in the {@link GroupShardsIterator} which is later
 * referred to as the {@code shardIndex}.
 * The fan out and collect algorithm is traditionally used as the initial phase which can either be a query execution or collection of
 * distributed frequencies
 */
abstract class InitialSearchPhase<FirstResult extends SearchPhaseResult> extends SearchPhase {
    private final SearchRequest request;
    private final GroupShardsIterator<SearchShardIterator> toSkipShardsIts;
    private final GroupShardsIterator<SearchShardIterator> shardsIts;
    private final Logger logger;
    private final int expectedTotalOps;
    private final AtomicInteger totalOps = new AtomicInteger();
    private final int maxConcurrentRequestsPerNode;
    private final Executor executor;
    private final Map<String, PendingExecutions> pendingExecutionsPerNode = new ConcurrentHashMap<>();
    private final boolean throttleConcurrentRequests;

    InitialSearchPhase(String name, SearchRequest request, GroupShardsIterator<SearchShardIterator> shardsIts, Logger logger,
                       int maxConcurrentRequestsPerNode, Executor executor) {
        super(name);
        this.request = request;
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
        this.shardsIts = new GroupShardsIterator<>(iterators);
        this.logger = logger;
        // we need to add 1 for non active partition, since we count it in the total. This means for each shard in the iterator we sum up
        // it's number of active shards but use 1 as the default if no replica of a shard is active at this point.
        // on a per shards level we use shardIt.remaining() to increment the totalOps pointer but add 1 for the current shard result
        // we process hence we add one for the non active partition here.
        this.expectedTotalOps = shardsIts.totalSizeWith1ForEmpty();
        this.maxConcurrentRequestsPerNode = maxConcurrentRequestsPerNode;
        // in the case were we have less shards than maxConcurrentRequestsPerNode we don't need to throttle
        this.throttleConcurrentRequests = maxConcurrentRequestsPerNode < shardsIts.size();
        this.executor = executor;
    }

    private void onShardFailure(final int shardIndex, @Nullable ShardRouting shard, @Nullable String nodeId,
                                final SearchShardIterator shardIt, Exception e) {
        // we always add the shard failure for a specific shard instance
        // we do make sure to clean it on a successful response from a shard
        SearchShardTarget shardTarget = shardIt.newSearchShardTarget(nodeId);
        onShardFailure(shardIndex, shardTarget, e);

        if (totalOps.incrementAndGet() == expectedTotalOps) {
            if (logger.isDebugEnabled()) {
                if (e != null && !TransportActions.isShardNotAvailableException(e)) {
                    logger.debug(new ParameterizedMessage(
                        "{}: Failed to execute [{}]", shard != null ? shard.shortSummary() : shardIt.shardId(), request), e);
                } else if (logger.isTraceEnabled()) {
                    logger.trace(new ParameterizedMessage("{}: Failed to execute [{}]", shard, request), e);
                }
            }
            onPhaseDone();
        } else {
            final ShardRouting nextShard = shardIt.nextOrNull();
            final boolean lastShard = nextShard == null;
            // trace log this exception
            logger.trace(() -> new ParameterizedMessage(
                "{}: Failed to execute [{}] lastShard [{}]",
                shard != null ? shard.shortSummary() : shardIt.shardId(), request, lastShard), e);
            if (!lastShard) {
                performPhaseOnShard(shardIndex, shardIt, nextShard);
            } else {
                // no more shards active, add a failure
                if (logger.isDebugEnabled() && !logger.isTraceEnabled()) { // do not double log this exception
                    if (e != null && !TransportActions.isShardNotAvailableException(e)) {
                        logger.debug(new ParameterizedMessage(
                            "{}: Failed to execute [{}] lastShard [{}]",
                            shard != null ? shard.shortSummary() : shardIt.shardId(), request, lastShard), e);
                    }
                }
            }
        }
    }

    @Override
    public final void run() {
        for (final SearchShardIterator iterator : toSkipShardsIts) {
            assert iterator.skip();
            skipShard(iterator);
        }
        if (shardsIts.size() > 0) {
            assert request.allowPartialSearchResults() != null : "SearchRequest missing setting for allowPartialSearchResults";
            if (request.allowPartialSearchResults() == false) {
                final StringBuilder missingShards = new StringBuilder();
                // Fail-fast verification of all shards being available
                for (int index = 0; index < shardsIts.size(); index++) {
                    final SearchShardIterator shardRoutings = shardsIts.get(index);
                    if (shardRoutings.size() == 0) {
                        if(missingShards.length() > 0){
                            missingShards.append(", ");
                        }
                        missingShards.append(shardRoutings.shardId());
                    }
                }
                if (missingShards.length() > 0) {
                    //Status red - shard is missing all copies and would produce partial results for an index search
                    final String msg = "Search rejected due to missing shards ["+ missingShards +
                            "]. Consider using `allow_partial_search_results` setting to bypass this error.";
                    throw new SearchPhaseExecutionException(getName(), msg, null, ShardSearchFailure.EMPTY_ARRAY);
                }
            }
            for (int index = 0; index < shardsIts.size(); index++) {
                final SearchShardIterator shardRoutings = shardsIts.get(index);
                assert shardRoutings.skip() == false;
                performPhaseOnShard(index, shardRoutings, shardRoutings.nextOrNull());
            }
        }
    }

    private void fork(final Runnable runnable) {
        executor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {

            }

            @Override
            protected void doRun() {
                runnable.run();
            }

            @Override
            public boolean isForceExecution() {
                // we can not allow a stuffed queue to reject execution here
                return true;
            }
        });
    }

    private static final class PendingExecutions {
        private final int permits;
        private int permitsTaken = 0;
        private ArrayDeque<Runnable> queue = new ArrayDeque<>();

        PendingExecutions(int permits) {
            assert permits > 0 : "not enough permits: " + permits;
            this.permits = permits;
        }

        void finishAndRunNext() {
            synchronized (this) {
                permitsTaken--;
                assert permitsTaken >= 0 : "illegal taken permits: " + permitsTaken;
            }
            tryRun(null);
        }

        void tryRun(Runnable runnable) {
            Runnable r = tryQueue(runnable);
            if (r != null) {
                r.run();
            }
        }

        private synchronized Runnable tryQueue(Runnable runnable) {
            Runnable toExecute = null;
            if (permitsTaken < permits) {
                permitsTaken++;
                toExecute = runnable;
                if (toExecute == null) { // only poll if we don't have anything to execute
                    toExecute = queue.poll();
                }
                if (toExecute == null) {
                    permitsTaken--;
                }
            } else if (runnable != null) {
                queue.add(runnable);
            }
            return toExecute;
        }
    }

    private void executeNext(PendingExecutions pendingExecutions, Thread originalThread) {
        executeNext(pendingExecutions == null ? null : pendingExecutions::finishAndRunNext, originalThread);
    }

    protected void executeNext(Runnable runnable, Thread originalThread) {
        if (throttleConcurrentRequests) {
            if (originalThread == Thread.currentThread()) {
                fork(runnable);
            } else {
                runnable.run();
            }
        } else {
            assert runnable == null;
        }
    }

    private void performPhaseOnShard(final int shardIndex, final SearchShardIterator shardIt, final ShardRouting shard) {
        /*
         * We capture the thread that this phase is starting on. When we are called back after executing the phase, we are either on the
         * same thread (because we never went async, or the same thread was selected from the thread pool) or a different thread. If we
         * continue on the same thread in the case that we never went async and this happens repeatedly we will end up recursing deeply and
         * could stack overflow. To prevent this, we fork if we are called back on the same thread that execution started on and otherwise
         * we can continue (cf. InitialSearchPhase#maybeFork).
         */
        if (shard == null) {
            fork(() -> onShardFailure(shardIndex, null, null, shardIt, new NoShardAvailableActionException(shardIt.shardId())));
        } else {
            final PendingExecutions pendingExecutions = throttleConcurrentRequests ?
                pendingExecutionsPerNode.computeIfAbsent(shard.currentNodeId(), n -> new PendingExecutions(maxConcurrentRequestsPerNode))
                : null;
            Runnable r = () -> {
                final Thread thread = Thread.currentThread();
                try {
                    executePhaseOnShard(shardIt, shard,
                        new SearchActionListener<FirstResult>(shardIt.newSearchShardTarget(shard.currentNodeId()), shardIndex) {
                            @Override
                            public void innerOnResponse(FirstResult result) {
                                try {
                                    onShardResult(result, shardIt);
                                } finally {
                                    executeNext(pendingExecutions, thread);
                                }
                            }

                            @Override
                            public void onFailure(Exception t) {
                                try {
                                    onShardFailure(shardIndex, shard, shard.currentNodeId(), shardIt, t);
                                } finally {
                                    executeNext(pendingExecutions, thread);
                                }
                            }
                        });
                } catch (final Exception e) {
                    try {
                        /*
                         * It is possible to run into connection exceptions here because we are getting the connection early and might
                         * run into nodes that are not connected. In this case, on shard failure will move us to the next shard copy.
                         */
                        fork(() -> onShardFailure(shardIndex, shard, shard.currentNodeId(), shardIt, e));
                    } finally {
                        executeNext(pendingExecutions, thread);
                    }
                }
            };
            if (throttleConcurrentRequests) {
                pendingExecutions.tryRun(r);
            } else {
                r.run();
            }
        }
    }

    private void onShardResult(FirstResult result, SearchShardIterator shardIt) {
        assert result.getShardIndex() != -1 : "shard index is not set";
        assert result.getSearchShardTarget() != null : "search shard target must not be null";
        onShardSuccess(result);
        // we need to increment successful ops first before we compare the exit condition otherwise if we
        // are fast we could concurrently update totalOps but then preempt one of the threads which can
        // cause the successor to read a wrong value from successfulOps if second phase is very fast ie. count etc.
        // increment all the "future" shards to update the total ops since we some may work and some may not...
        // and when that happens, we break on total ops, so we must maintain them
        successfulShardExecution(shardIt);
    }

    private void successfulShardExecution(SearchShardIterator shardsIt) {
        final int remainingOpsOnIterator;
        if (shardsIt.skip()) {
            remainingOpsOnIterator = shardsIt.remaining();
        } else {
            remainingOpsOnIterator = shardsIt.remaining() + 1;
        }
        final int xTotalOps = totalOps.addAndGet(remainingOpsOnIterator);
        if (xTotalOps == expectedTotalOps) {
            onPhaseDone();
        } else if (xTotalOps > expectedTotalOps) {
            throw new AssertionError("unexpected higher total ops [" + xTotalOps + "] compared to expected ["
                + expectedTotalOps + "]");
        }
    }

    /**
     * Executed once all shard results have been received and processed
     * @see #onShardFailure(int, SearchShardTarget, Exception)
     * @see #onShardSuccess(SearchPhaseResult)
     */
    abstract void onPhaseDone(); // as a tribute to @kimchy aka. finishHim()

    /**
     * Executed once for every failed shard level request. This method is invoked before the next replica is tried for the given
     * shard target.
     * @param shardIndex the internal index for this shard. Each shard has an index / ordinal assigned that is used to reference
     *                   it's results
     * @param shardTarget the shard target for this failure
     * @param ex the failure reason
     */
    abstract void onShardFailure(int shardIndex, SearchShardTarget shardTarget, Exception ex);

    /**
     * Executed once for every successful shard level request.
     * @param result the result returned form the shard
     *
     */
    abstract void onShardSuccess(FirstResult result);

    /**
     * Sends the request to the actual shard.
     * @param shardIt the shards iterator
     * @param shard the shard routing to send the request for
     * @param listener the listener to notify on response
     */
    protected abstract void executePhaseOnShard(SearchShardIterator shardIt, ShardRouting shard,
                                                SearchActionListener<FirstResult> listener);

    /**
     * This class acts as a basic result collection that can be extended to do on-the-fly reduction or result processing
     */
    abstract static class SearchPhaseResults<Result extends SearchPhaseResult> {
        private final int numShards;

        SearchPhaseResults(int numShards) {
            this.numShards = numShards;
        }
        /**
         * Returns the number of expected results this class should collect
         */
        final int getNumShards() {
            return numShards;
        }

        /**
         * A stream of all non-null (successful) shard results
         */
        abstract Stream<Result> getSuccessfulResults();

        /**
         * Consumes a single shard result
         * @param result the shards result
         */
        abstract void consumeResult(Result result);

        /**
         * Returns <code>true</code> iff a result if present for the given shard ID.
         */
        abstract boolean hasResult(int shardIndex);

        void consumeShardFailure(int shardIndex) {}

        AtomicArray<Result> getAtomicArray() {
            throw new UnsupportedOperationException();
        }

        /**
         * Reduces the collected results
         */
        SearchPhaseController.ReducedQueryPhase reduce() {
            throw new UnsupportedOperationException("reduce is not supported");
        }
    }

    /**
     * This class acts as a basic result collection that can be extended to do on-the-fly reduction or result processing
     */
    static class ArraySearchPhaseResults<Result extends SearchPhaseResult> extends SearchPhaseResults<Result> {
        final AtomicArray<Result> results;

        ArraySearchPhaseResults(int size) {
            super(size);
            this.results = new AtomicArray<>(size);
        }

        Stream<Result> getSuccessfulResults() {
            return results.asList().stream();
        }

        void consumeResult(Result result) {
            assert results.get(result.getShardIndex()) == null : "shardIndex: " + result.getShardIndex() + " is already set";
            results.set(result.getShardIndex(), result);
        }

        boolean hasResult(int shardIndex) {
            return results.get(shardIndex) != null;
        }

        @Override
        AtomicArray<Result> getAtomicArray() {
            return results;
        }
    }

    protected void skipShard(SearchShardIterator iterator) {
        assert iterator.skip();
        successfulShardExecution(iterator);
    }
}
