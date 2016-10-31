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

import com.carrotsearch.hppc.IntArrayList;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;


abstract class AbstractSearchAsyncAction<FirstResult extends SearchPhaseResult> extends AbstractAsyncAction {

    protected final Logger logger;
    protected final SearchTransportService searchTransportService;
    private final Executor executor;
    protected final ActionListener<SearchResponse> listener;
    private final GroupShardsIterator shardsIts;
    protected final SearchRequest request;
    /** Used by subclasses to resolve node ids to DiscoveryNodes. **/
    protected final Function<String, DiscoveryNode> nodeIdToDiscoveryNode;
    protected final SearchTask task;
    protected final int expectedSuccessfulOps;
    private final int expectedTotalOps;
    protected final AtomicInteger successfulOps = new AtomicInteger();
    private final AtomicInteger totalOps = new AtomicInteger();
    protected final AtomicArray<FirstResult> firstResults;
    private final Map<String, AliasFilter> aliasFilter;
    private final long clusterStateVersion;
    private volatile AtomicArray<ShardSearchFailure> shardFailures;
    private final Object shardFailuresMutex = new Object();
    protected volatile ScoreDoc[] sortedShardDocs;

    protected AbstractSearchAsyncAction(Logger logger, SearchTransportService searchTransportService,
                                        Function<String, DiscoveryNode> nodeIdToDiscoveryNode,
                                        Map<String, AliasFilter> aliasFilter, Executor executor, SearchRequest request,
                                        ActionListener<SearchResponse> listener, GroupShardsIterator shardsIts, long startTime,
                                        long clusterStateVersion, SearchTask task) {
        super(startTime);
        this.logger = logger;
        this.searchTransportService = searchTransportService;
        this.executor = executor;
        this.request = request;
        this.task = task;
        this.listener = listener;
        this.nodeIdToDiscoveryNode = nodeIdToDiscoveryNode;
        this.clusterStateVersion = clusterStateVersion;
        this.shardsIts = shardsIts;
        expectedSuccessfulOps = shardsIts.size();
        // we need to add 1 for non active partition, since we count it in the total!
        expectedTotalOps = shardsIts.totalSizeWith1ForEmpty();
        firstResults = new AtomicArray<>(shardsIts.size());
        this.aliasFilter = aliasFilter;
    }



    public void start() {
        if (expectedSuccessfulOps == 0) {
            //no search shards to search on, bail with empty response
            //(it happens with search across _all with no indices around and consistent with broadcast operations)
            listener.onResponse(new SearchResponse(InternalSearchResponse.empty(), null, 0, 0, buildTookInMillis(),
                ShardSearchFailure.EMPTY_ARRAY));
            return;
        }
        int shardIndex = -1;
        for (final ShardIterator shardIt : shardsIts) {
            shardIndex++;
            final ShardRouting shard = shardIt.nextOrNull();
            if (shard != null) {
                performFirstPhase(shardIndex, shardIt, shard);
            } else {
                // really, no shards active in this group
                onFirstPhaseResult(shardIndex, null, null, shardIt, new NoShardAvailableActionException(shardIt.shardId()));
            }
        }
    }

    void performFirstPhase(final int shardIndex, final ShardIterator shardIt, final ShardRouting shard) {
        if (shard == null) {
            // no more active shards... (we should not really get here, but just for safety)
            onFirstPhaseResult(shardIndex, null, null, shardIt, new NoShardAvailableActionException(shardIt.shardId()));
        } else {
            final DiscoveryNode node = nodeIdToDiscoveryNode.apply(shard.currentNodeId());
            if (node == null) {
                onFirstPhaseResult(shardIndex, shard, null, shardIt, new NoShardAvailableActionException(shardIt.shardId()));
            } else {
                AliasFilter filter = this.aliasFilter.get(shard.index().getName());
                ShardSearchTransportRequest transportRequest = new ShardSearchTransportRequest(request, shard, shardsIts.size(),
                    filter, startTime());
                sendExecuteFirstPhase(node, transportRequest , new ActionListener<FirstResult>() {
                        @Override
                        public void onResponse(FirstResult result) {
                            onFirstPhaseResult(shardIndex, shard, result, shardIt);
                        }

                        @Override
                        public void onFailure(Exception t) {
                            onFirstPhaseResult(shardIndex, shard, node.getId(), shardIt, t);
                        }
                    });
            }
        }
    }

    void onFirstPhaseResult(int shardIndex, ShardRouting shard, FirstResult result, ShardIterator shardIt) {
        result.shardTarget(new SearchShardTarget(shard.currentNodeId(), shard.index(), shard.id()));
        processFirstPhaseResult(shardIndex, result);
        // we need to increment successful ops first before we compare the exit condition otherwise if we
        // are fast we could concurrently update totalOps but then preempt one of the threads which can
        // cause the successor to read a wrong value from successfulOps if second phase is very fast ie. count etc.
        successfulOps.incrementAndGet();
        // increment all the "future" shards to update the total ops since we some may work and some may not...
        // and when that happens, we break on total ops, so we must maintain them
        final int xTotalOps = totalOps.addAndGet(shardIt.remaining() + 1);
        if (xTotalOps == expectedTotalOps) {
            try {
                innerMoveToSecondPhase();
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "{}: Failed to execute [{}] while moving to second phase",
                            shardIt.shardId(),
                            request),
                        e);
                }
                raiseEarlyFailure(new ReduceSearchPhaseException(firstPhaseName(), "", e, buildShardFailures()));
            }
        } else if (xTotalOps > expectedTotalOps) {
            raiseEarlyFailure(new IllegalStateException("unexpected higher total ops [" + xTotalOps + "] compared " +
                "to expected [" + expectedTotalOps + "]"));
        }
    }

    void onFirstPhaseResult(final int shardIndex, @Nullable ShardRouting shard, @Nullable String nodeId,
                            final ShardIterator shardIt, Exception e) {
        // we always add the shard failure for a specific shard instance
        // we do make sure to clean it on a successful response from a shard
        SearchShardTarget shardTarget = new SearchShardTarget(nodeId, shardIt.shardId().getIndex(), shardIt.shardId().getId());
        addShardFailure(shardIndex, shardTarget, e);

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
            final ShardSearchFailure[] shardSearchFailures = buildShardFailures();
            if (successfulOps.get() == 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug((Supplier<?>) () -> new ParameterizedMessage("All shards failed for phase: [{}]", firstPhaseName()), e);
                }

                // no successful ops, raise an exception
                raiseEarlyFailure(new SearchPhaseExecutionException(firstPhaseName(), "all shards failed", e, shardSearchFailures));
            } else {
                try {
                    innerMoveToSecondPhase();
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    raiseEarlyFailure(new ReduceSearchPhaseException(firstPhaseName(), "", inner, shardSearchFailures));
                }
            }
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
                    performFirstPhase(shardIndex, shardIt, nextShard);
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    onFirstPhaseResult(shardIndex, shard, shard.currentNodeId(), shardIt, inner);
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

    protected final ShardSearchFailure[] buildShardFailures() {
        AtomicArray<ShardSearchFailure> shardFailures = this.shardFailures;
        if (shardFailures == null) {
            return ShardSearchFailure.EMPTY_ARRAY;
        }
        List<AtomicArray.Entry<ShardSearchFailure>> entries = shardFailures.asList();
        ShardSearchFailure[] failures = new ShardSearchFailure[entries.size()];
        for (int i = 0; i < failures.length; i++) {
            failures[i] = entries.get(i).value;
        }
        return failures;
    }

    protected final void addShardFailure(final int shardIndex, @Nullable SearchShardTarget shardTarget, Exception e) {
        // we don't aggregate shard failures on non active shards (but do keep the header counts right)
        if (TransportActions.isShardNotAvailableException(e)) {
            return;
        }

        // lazily create shard failures, so we can early build the empty shard failure list in most cases (no failures)
        if (shardFailures == null) {
            synchronized (shardFailuresMutex) {
                if (shardFailures == null) {
                    shardFailures = new AtomicArray<>(shardsIts.size());
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
    }

    private void raiseEarlyFailure(Exception e) {
        for (AtomicArray.Entry<FirstResult> entry : firstResults.asList()) {
            try {
                DiscoveryNode node = nodeIdToDiscoveryNode.apply(entry.value.shardTarget().nodeId());
                sendReleaseSearchContext(entry.value.id(), node);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.trace("failed to release context", inner);
            }
        }
        listener.onFailure(e);
    }

    /**
     * Releases shard targets that are not used in the docsIdsToLoad.
     */
    protected void releaseIrrelevantSearchContexts(AtomicArray<? extends QuerySearchResultProvider> queryResults,
                                                   AtomicArray<IntArrayList> docIdsToLoad) {
        if (docIdsToLoad == null) {
            return;
        }
        // we only release search context that we did not fetch from if we are not scrolling
        if (request.scroll() == null) {
            for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : queryResults.asList()) {
                QuerySearchResult queryResult = entry.value.queryResult();
                if (queryResult.hasHits()
                    && docIdsToLoad.get(entry.index) == null) { // but none of them made it to the global top docs
                    try {
                        DiscoveryNode node = nodeIdToDiscoveryNode.apply(entry.value.queryResult().shardTarget().nodeId());
                        sendReleaseSearchContext(entry.value.queryResult().id(), node);
                    } catch (Exception e) {
                        logger.trace("failed to release context", e);
                    }
                }
            }
        }
    }

    protected void sendReleaseSearchContext(long contextId, DiscoveryNode node) {
        if (node != null) {
            searchTransportService.sendFreeContext(node, contextId, request);
        }
    }

    protected ShardFetchSearchRequest createFetchRequest(QuerySearchResult queryResult, AtomicArray.Entry<IntArrayList> entry,
                                                         ScoreDoc[] lastEmittedDocPerShard) {
        final ScoreDoc lastEmittedDoc = (lastEmittedDocPerShard != null) ? lastEmittedDocPerShard[entry.index] : null;
        return new ShardFetchSearchRequest(request, queryResult.id(), entry.value, lastEmittedDoc);
    }

    protected abstract void sendExecuteFirstPhase(DiscoveryNode node, ShardSearchTransportRequest request,
                                                  ActionListener<FirstResult> listener);

    protected final void processFirstPhaseResult(int shardIndex, FirstResult result) {
        firstResults.set(shardIndex, result);

        if (logger.isTraceEnabled()) {
            logger.trace("got first-phase result from {}", result != null ? result.shardTarget() : null);
        }

        // clean a previous error on this shard group (note, this code will be serialized on the same shardIndex value level
        // so its ok concurrency wise to miss potentially the shard failures being created because of another failure
        // in the #addShardFailure, because by definition, it will happen on *another* shardIndex
        AtomicArray<ShardSearchFailure> shardFailures = this.shardFailures;
        if (shardFailures != null) {
            shardFailures.set(shardIndex, null);
        }
    }

    final void innerMoveToSecondPhase() throws Exception {
        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder();
            boolean hadOne = false;
            for (int i = 0; i < firstResults.length(); i++) {
                FirstResult result = firstResults.get(i);
                if (result == null) {
                    continue; // failure
                }
                if (hadOne) {
                    sb.append(",");
                } else {
                    hadOne = true;
                }
                sb.append(result.shardTarget());
            }

            logger.trace("Moving to second phase, based on results from: {} (cluster state version: {})", sb, clusterStateVersion);
        }
        moveToSecondPhase();
    }

    protected abstract void moveToSecondPhase() throws Exception;

    protected abstract String firstPhaseName();

    protected Executor getExecutor() {
        return executor;
    }

}
