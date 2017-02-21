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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
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
    private final GroupShardsIterator shardsIts;
    private final Logger logger;
    private final int expectedTotalOps;
    private final AtomicInteger totalOps = new AtomicInteger();

    InitialSearchPhase(String name, SearchRequest request, GroupShardsIterator shardsIts, Logger logger) {
        super(name);
        this.request = request;
        this.shardsIts = shardsIts;
        this.logger = logger;
        // we need to add 1 for non active partition, since we count it in the total. This means for each shard in the iterator we sum up
        // it's number of active shards but use 1 as the default if no replica of a shard is active at this point.
        // on a per shards level we use shardIt.remaining() to increment the totalOps pointer but add 1 for the current shard result
        // we process hence we add one for the non active partition here.
        this.expectedTotalOps = shardsIts.totalSizeWith1ForEmpty();
    }

    private void onShardFailure(final int shardIndex, @Nullable ShardRouting shard, @Nullable String nodeId,
                                final ShardIterator shardIt, Exception e) {
        // we always add the shard failure for a specific shard instance
        // we do make sure to clean it on a successful response from a shard
        SearchShardTarget shardTarget = new SearchShardTarget(nodeId, shardIt.shardId());
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
        int shardIndex = -1;
        for (final ShardIterator shardIt : shardsIts) {
            shardIndex++;
            final ShardRouting shard = shardIt.nextOrNull();
            if (shard != null) {
                performPhaseOnShard(shardIndex, shardIt, shard);
            } else {
                // really, no shards active in this group
                onShardFailure(shardIndex, null, null, shardIt, new NoShardAvailableActionException(shardIt.shardId()));
            }
        }
    }

    private void performPhaseOnShard(final int shardIndex, final ShardIterator shardIt, final ShardRouting shard) {
        if (shard == null) {
            // TODO upgrade this to an assert...
            // no more active shards... (we should not really get here, but just for safety)
            onShardFailure(shardIndex, null, null, shardIt, new NoShardAvailableActionException(shardIt.shardId()));
        } else {
            try {
                executePhaseOnShard(shardIt, shard, new ActionListener<FirstResult>() {
                    @Override
                    public void onResponse(FirstResult result) {
                        onShardResult(shardIndex, shard.currentNodeId(), result, shardIt);
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

    private void onShardResult(int shardIndex, String nodeId, FirstResult result, ShardIterator shardIt) {
        result.shardTarget(new SearchShardTarget(nodeId, shardIt.shardId()));
        onShardSuccess(shardIndex, result);
        // we need to increment successful ops first before we compare the exit condition otherwise if we
        // are fast we could concurrently update totalOps but then preempt one of the threads which can
        // cause the successor to read a wrong value from successfulOps if second phase is very fast ie. count etc.
        // increment all the "future" shards to update the total ops since we some may work and some may not...
        // and when that happens, we break on total ops, so we must maintain them
        final int xTotalOps = totalOps.addAndGet(shardIt.remaining() + 1);
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
     * @see #onShardSuccess(int, SearchPhaseResult)
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
     * @param shardIndex the internal index for this shard. Each shard has an index / ordinal assigned that is used to reference
     *                   it's results
     * @param result the result returned form the shard
     *
     */
    abstract void onShardSuccess(int shardIndex, FirstResult result);

    /**
     * Sends the request to the actual shard.
     * @param shardIt the shards iterator
     * @param shard the shard routing to send the request for
     * @param listener the listener to notify on response
     */
    protected abstract void executePhaseOnShard(ShardIterator shardIt, ShardRouting shard, ActionListener<FirstResult> listener);

    /**
     * This class acts as a basic result collection that can be extended to do on-the-fly reduction or result processing
     */
    static class SearchPhaseResults<Result extends SearchPhaseResult> {
        final AtomicArray<Result> results;

        SearchPhaseResults(int size) {
            results = new AtomicArray<>(size);
        }

        /**
         * Returns the number of expected results this class should collect
         */
        final int getNumShards() {
            return results.length();
        }

        /**
         * A stream of all non-null (successful) shard results
         */
        final Stream<Result> getSuccessfulResults() {
            return results.asList().stream().map(e -> e.value);
        }

        /**
         * Consumes a single shard result
         * @param shardIndex the shards index, this is a 0-based id that is used to establish a 1 to 1 mapping to the searched shards
         * @param result the shards result
         */
        void consumeResult(int shardIndex, Result result) {
            assert results.get(shardIndex) == null : "shardIndex: " + shardIndex + " is already set";
            results.set(shardIndex, result);
        }

        /**
         * Returns <code>true</code> iff a result if present for the given shard ID.
         */
        final boolean hasResult(int shardIndex) {
            return results.get(shardIndex) != null;
        }

        /**
         * Reduces the collected results
         */
        SearchPhaseController.ReducedQueryPhase reduce() {
            throw new UnsupportedOperationException("reduce is not supported");
        }
    }
}
