/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.search.type;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.BaseAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.trove.ExtTIntArrayList;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.controller.ShardDoc;
import org.elasticsearch.search.internal.InternalSearchRequest;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.search.type.TransportSearchHelper.*;

/**
 * @author kimchy (shay.banon)
 */
public abstract class TransportSearchTypeAction extends BaseAction<SearchRequest, SearchResponse> {

    protected final ClusterService clusterService;

    protected final SearchServiceTransportAction searchService;

    protected final SearchPhaseController searchPhaseController;

    protected final TransportSearchCache searchCache;

    public TransportSearchTypeAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                     TransportSearchCache searchCache, SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController) {
        super(settings, threadPool);
        this.clusterService = clusterService;
        this.searchCache = searchCache;
        this.searchService = searchService;
        this.searchPhaseController = searchPhaseController;
    }

    protected abstract class BaseAsyncAction<FirstResult extends SearchPhaseResult> {

        protected final ActionListener<SearchResponse> listener;

        protected final GroupShardsIterator shardsIts;

        protected final SearchRequest request;

        protected final DiscoveryNodes nodes;

        protected final int expectedSuccessfulOps;

        protected final int expectedTotalOps;

        protected final AtomicInteger successulOps = new AtomicInteger();

        protected final AtomicInteger totalOps = new AtomicInteger();

        protected final Collection<ShardSearchFailure> shardFailures = searchCache.obtainShardFailures();

        protected volatile ShardDoc[] sortedShardList;

        protected final long startTime = System.currentTimeMillis();

        protected BaseAsyncAction(SearchRequest request, ActionListener<SearchResponse> listener) {
            this.request = request;
            this.listener = listener;

            ClusterState clusterState = clusterService.state();

            nodes = clusterState.nodes();

            request.indices(clusterState.metaData().concreteIndices(request.indices()));

            for (String index : request.indices()) {
                clusterState.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, index);
            }

            shardsIts = clusterService.operationRouting().searchShards(clusterState, request.indices(), request.queryHint(), request.routing(), request.preference());
            expectedSuccessfulOps = shardsIts.size();
            // we need to add 1 for non active partition, since we count it in the total!
            expectedTotalOps = shardsIts.totalSizeActiveWith1ForEmpty();

            if (expectedSuccessfulOps == 0) {
                // not search shards to search on...
                throw new SearchPhaseExecutionException("initial", "No indices / shards to search on, requested indices are " + Arrays.toString(request.indices()), buildShardFailures());
            }
        }

        public void start() {
            // count the local operations, and perform the non local ones
            int localOperations = 0;
            for (final ShardIterator shardIt : shardsIts) {
                final ShardRouting shard = shardIt.nextActiveOrNull();
                if (shard != null) {
                    if (shard.currentNodeId().equals(nodes.localNodeId())) {
                        localOperations++;
                    } else {
                        // do the remote operation here, the localAsync flag is not relevant
                        performFirstPhase(shardIt.reset());
                    }
                } else {
                    // really, no shards active in this group
                    onFirstPhaseResult(null, shardIt, null);
                }
            }
            // we have local operations, perform them now
            if (localOperations > 0) {
                if (request.operationThreading() == SearchOperationThreading.SINGLE_THREAD) {
                    request.beforeLocalFork();
                    threadPool.executor(ThreadPool.Names.SEARCH).execute(new Runnable() {
                        @Override public void run() {
                            for (final ShardIterator shardIt : shardsIts) {
                                final ShardRouting shard = shardIt.reset().nextActiveOrNull();
                                if (shard != null) {
                                    if (shard.currentNodeId().equals(nodes.localNodeId())) {
                                        performFirstPhase(shardIt.reset());
                                    }
                                }
                            }
                        }
                    });
                } else {
                    boolean localAsync = request.operationThreading() == SearchOperationThreading.THREAD_PER_SHARD;
                    if (localAsync) {
                        request.beforeLocalFork();
                    }
                    for (final ShardIterator shardIt : shardsIts) {
                        final ShardRouting shard = shardIt.reset().nextActiveOrNull();
                        if (shard != null) {
                            if (shard.currentNodeId().equals(nodes.localNodeId())) {
                                if (localAsync) {
                                    threadPool.executor(ThreadPool.Names.SEARCH).execute(new Runnable() {
                                        @Override public void run() {
                                            performFirstPhase(shardIt.reset());
                                        }
                                    });
                                } else {
                                    performFirstPhase(shardIt.reset());
                                }
                            }
                        }
                    }
                }
            }
        }

        private void performFirstPhase(final ShardIterator shardIt) {
            final ShardRouting shard = shardIt.nextActiveOrNull();
            if (shard == null) {
                // no more active shards... (we should not really get here, but just for safety)
                onFirstPhaseResult(null, shardIt, null);
            } else {
                DiscoveryNode node = nodes.get(shard.currentNodeId());
                if (node == null) {
                    onFirstPhaseResult(shard, shardIt, null);
                } else {
                    sendExecuteFirstPhase(node, internalSearchRequest(shard, shardsIts.size(), request), new SearchServiceListener<FirstResult>() {
                        @Override public void onResult(FirstResult result) {
                            onFirstPhaseResult(shard, result, shardIt);
                        }

                        @Override public void onFailure(Throwable t) {
                            onFirstPhaseResult(shard, shardIt, t);
                        }
                    });
                }
            }
        }

        private void onFirstPhaseResult(ShardRouting shard, FirstResult result, ShardIterator shardIt) {
            result.shardTarget(new SearchShardTarget(shard.currentNodeId(), shard.index(), shard.id()));
            processFirstPhaseResult(shard, result);
            // increment all the "future" shards to update the total ops since we some may work and some may not...
            // and when that happens, we break on total ops, so we must maintain them
            while (shardIt.hasNextActive()) {
                totalOps.incrementAndGet();
                shardIt.nextActive();
            }
            if (successulOps.incrementAndGet() == expectedSuccessfulOps ||
                    totalOps.incrementAndGet() == expectedTotalOps) {
                try {
                    moveToSecondPhase();
                } catch (Exception e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(shardIt.shardId() + ": Failed to execute [" + request + "] while moving to second phase", e);
                    }
                    listener.onFailure(new ReduceSearchPhaseException(firstPhaseName(), "", e, buildShardFailures()));
                }
            }
        }

        private void onFirstPhaseResult(@Nullable ShardRouting shard, final ShardIterator shardIt, Throwable t) {
            if (totalOps.incrementAndGet() == expectedTotalOps) {
                // e is null when there is no next active....
                if (logger.isDebugEnabled()) {
                    if (t != null) {
                        if (shard != null) {
                            logger.debug(shard.shortSummary() + ": Failed to execute [" + request + "]", t);
                        } else {
                            logger.debug(shardIt.shardId() + ": Failed to execute [" + request + "]", t);
                        }
                    }
                }
                // no more shards, add a failure
                if (t == null) {
                    // no active shards
                    shardFailures.add(new ShardSearchFailure("No active shards", new SearchShardTarget(null, shardIt.shardId().index().name(), shardIt.shardId().id())));
                } else {
                    shardFailures.add(new ShardSearchFailure(t));
                }
                if (successulOps.get() == 0) {
                    // no successful ops, raise an exception
                    listener.onFailure(new SearchPhaseExecutionException(firstPhaseName(), "total failure", buildShardFailures()));
                } else {
                    try {
                        moveToSecondPhase();
                    } catch (Exception e) {
                        listener.onFailure(new ReduceSearchPhaseException(firstPhaseName(), "", e, buildShardFailures()));
                    }
                }
            } else {
                if (shardIt.hasNextActive()) {
                    // trace log this exception
                    if (logger.isTraceEnabled()) {
                        if (t != null) {
                            if (shard != null) {
                                logger.trace(shard.shortSummary() + ": Failed to execute [" + request + "]", t);
                            } else {
                                logger.trace(shardIt.shardId() + ": Failed to execute [" + request + "]", t);
                            }
                        }
                    }
                    performFirstPhase(shardIt);
                } else {
                    // no more shards active, add a failure
                    // e is null when there is no next active....
                    if (logger.isDebugEnabled()) {
                        if (t != null) {
                            if (shard != null) {
                                logger.debug(shard.shortSummary() + ": Failed to execute [" + request + "]", t);
                            } else {
                                logger.debug(shardIt.shardId() + ": Failed to execute [" + request + "]", t);
                            }
                        }
                    }
                    if (t == null) {
                        // no active shards
                        shardFailures.add(new ShardSearchFailure("No active shards", new SearchShardTarget(null, shardIt.shardId().index().name(), shardIt.shardId().id())));
                    } else {
                        shardFailures.add(new ShardSearchFailure(t));
                    }
                }
            }
        }

        /**
         * Builds how long it took to execute the search.
         */
        protected final long buildTookInMillis() {
            return System.currentTimeMillis() - startTime;
        }

        /**
         * Builds the shard failures, and releases the cache (meaning this should only be called once!).
         */
        protected final ShardSearchFailure[] buildShardFailures() {
            return TransportSearchHelper.buildShardFailures(shardFailures, searchCache);
        }

        /**
         * Releases shard targets that are not used in the docsIdsToLoad.
         */
        protected void releaseIrrelevantSearchContexts(Map<SearchShardTarget, QuerySearchResultProvider> queryResults,
                                                       Map<SearchShardTarget, ExtTIntArrayList> docIdsToLoad) {
            if (docIdsToLoad == null) {
                return;
            }
            // we only release search context that we did not fetch from if we are not scrolling
            if (request.scroll() == null) {
                for (Map.Entry<SearchShardTarget, QuerySearchResultProvider> entry : queryResults.entrySet()) {
                    if (!docIdsToLoad.containsKey(entry.getKey())) {
                        DiscoveryNode node = nodes.get(entry.getKey().nodeId());
                        if (node != null) { // should not happen (==null) but safeguard anyhow
                            searchService.sendFreeContext(node, entry.getValue().id());
                        }
                    }
                }
            }
        }

        protected abstract void sendExecuteFirstPhase(DiscoveryNode node, InternalSearchRequest request, SearchServiceListener<FirstResult> listener);

        protected abstract void processFirstPhaseResult(ShardRouting shard, FirstResult result);

        protected abstract void moveToSecondPhase() throws Exception;

        protected abstract String firstPhaseName();
    }
}
