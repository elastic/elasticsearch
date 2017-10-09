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
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.transport.ConnectTransportException;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * This is an abstract base class that encapsulates the logic to fan out to all shards in provided {@link GroupShardsIterator}
 * and collect the results. If a shard request returns a failure this class handles the advance to the next replica of the shard until
 * the shards replica iterator is exhausted. Each shard is referenced by position in the {@link GroupShardsIterator} which is later
 * referred to as the <tt>shardIndex</tt>.
 * The fan out and collect algorithm is traditionally used as the initial phase which can either be a query execution or collection
 * distributed frequencies
 */
abstract class InitialSearchPhase<FirstResult extends SearchPhaseResult> extends SearchPhase {
    private final SearchRequest request;
    private final GroupShardsIterator<SearchShardIterator> shardsIts;
    private final Logger logger;
    private final int expectedTotalOps;
    private final AtomicInteger totalOps = new AtomicInteger();
    private final AtomicInteger shardExecutionIndex = new AtomicInteger(0);
    private final int maxConcurrentShardRequests;

    InitialSearchPhase(String name, SearchRequest request, GroupShardsIterator<SearchShardIterator> shardsIts, Logger logger,
                       int maxConcurrentShardRequests) {
        super(name);
        this.request = request;
        this.shardsIts = shardsIts;
        this.logger = logger;
        // we need to add 1 for non active partition, since we count it in the total. This means for each shard in the iterator we sum up
        // it's number of active shards but use 1 as the default if no replica of a shard is active at this point.
        // on a per shards level we use shardIt.remaining() to increment the totalOps pointer but add 1 for the current shard result
        // we process hence we add one for the non active partition here.
        this.expectedTotalOps = shardsIts.totalSizeWith1ForEmpty();
        this.maxConcurrentShardRequests = Math.min(maxConcurrentShardRequests, shardsIts.size());
    }

    private void onShardFailure(final int shardIndex, @Nullable ShardRouting shard, @Nullable String nodeId,
                                final SearchShardIterator shardIt, Exception e) {
        // we always add the shard failure for a specific shard instance
        // we do make sure to clean it on a successful response from a shard
        SearchShardTarget shardTarget = new SearchShardTarget(nodeId, shardIt.shardId(), shardIt.getClusterAlias(),
            shardIt.getOriginalIndices());
        onShardFailure(shardIndex, shardTarget, e);

        if (totalOps.incrementAndGet() == expectedTotalOps) {
            if (logger.isDebugEnabled()) {
                if (e != null && !TransportActions.isShardNotAvailableException(e)) {
                    logger.debug(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "{}: Failed to execute [{}]",
                            shard != null ? shard.shortSummary() :
                                shardIt.shardId(),
                            request),
                        e);
                } else if (logger.isTraceEnabled()) {
                    logger.trace((Supplier<?>) () -> new ParameterizedMessage("{}: Failed to execute [{}]", shard, request), e);
                }
            }
            onPhaseDone();
        } else {
            final ShardRouting nextShard = shardIt.nextOrNull();
            final boolean lastShard = nextShard == null;
            // trace log this exception
            logger.trace(
                (Supplier<?>) () -> new ParameterizedMessage(
                    "{}: Failed to execute [{}] lastShard [{}]",
                    shard != null ? shard.shortSummary() : shardIt.shardId(),
                    request,
                    lastShard),
                e);
            if (!lastShard) {
                try {
                    performPhaseOnShard(shardIndex, shardIt, nextShard);
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    onShardFailure(shardIndex, shard, shard.currentNodeId(), shardIt, inner);
                }
            } else {
                maybeExecuteNext(); // move to the next execution if needed
                // no more shards active, add a failure
                if (logger.isDebugEnabled() && !logger.isTraceEnabled()) { // do not double log this exception
                    if (e != null && !TransportActions.isShardNotAvailableException(e)) {
                        logger.debug(
                            (Supplier<?>) () -> new ParameterizedMessage(
                                "{}: Failed to execute [{}] lastShard [{}]",
                                shard != null ? shard.shortSummary() :
                                    shardIt.shardId(),
                                request,
                                lastShard),
                            e);
                    }
                }
            }
        }
    }

    @Override
    public final void run() throws IOException {
        boolean success = shardExecutionIndex.compareAndSet(0, maxConcurrentShardRequests);
        assert success;
        for (int i = 0; i < maxConcurrentShardRequests; i++) {
            SearchShardIterator shardRoutings = shardsIts.get(i);
            if (shardRoutings.skip()) {
                skipShard(shardRoutings);
            } else {
                performPhaseOnShard(i, shardRoutings, shardRoutings.nextOrNull());
            }
        }
    }

    private void maybeExecuteNext() {
        final int index = shardExecutionIndex.getAndIncrement();
        if (index < shardsIts.size()) {
            SearchShardIterator shardRoutings = shardsIts.get(index);
            if (shardRoutings.skip()) {
                skipShard(shardRoutings);
            } else {
                performPhaseOnShard(index, shardRoutings, shardRoutings.nextOrNull());
            }
        }
    }


    private void performPhaseOnShard(final int shardIndex, final SearchShardIterator shardIt, final ShardRouting shard) {
        if (shard == null) {
            onShardFailure(shardIndex, null, null, shardIt, new NoShardAvailableActionException(shardIt.shardId()));
        } else {
            try {
                executePhaseOnShard(shardIt, shard, new SearchActionListener<FirstResult>(new SearchShardTarget(shard.currentNodeId(),
                    shardIt.shardId(), shardIt.getClusterAlias(), shardIt.getOriginalIndices()), shardIndex) {
                    @Override
                    public void innerOnResponse(FirstResult result) {
                        onShardResult(result, shardIt);
                    }

                    @Override
                    public void onFailure(Exception t) {
                        onShardFailure(shardIndex, shard, shard.currentNodeId(), shardIt, t);
                    }
                });
            } catch (ConnectTransportException | IllegalArgumentException ex) {
                // we are getting the connection early here so we might run into nodes that are not connected. in that case we move on to
                // the next shard. previously when using discovery nodes here we had a special case for null when a node was not connected
                // at all which is not not needed anymore.
                onShardFailure(shardIndex, shard, shard.currentNodeId(), shardIt, ex);
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
        } else {
            maybeExecuteNext();
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

        protected SearchPhaseResults(int numShards) {
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
