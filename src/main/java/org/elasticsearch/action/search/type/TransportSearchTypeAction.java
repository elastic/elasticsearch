/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

import jsr166y.LinkedTransferQueue;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.TransportAction;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.search.type.TransportSearchHelper.internalSearchRequest;

/**
 *
 */
public abstract class TransportSearchTypeAction extends TransportAction<SearchRequest, SearchResponse> {

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

        private final GroupShardsIterator shardsIts;

        protected final SearchRequest request;

        protected final ClusterState clusterState;
        protected final DiscoveryNodes nodes;

        protected final int expectedSuccessfulOps;

        private final int expectedTotalOps;

        protected final AtomicInteger successulOps = new AtomicInteger();

        private final AtomicInteger totalOps = new AtomicInteger();

        private volatile LinkedTransferQueue<ShardSearchFailure> shardFailures;

        protected volatile ShardDoc[] sortedShardList;

        protected final long startTime = System.currentTimeMillis();

        protected BaseAsyncAction(SearchRequest request, ActionListener<SearchResponse> listener) {
            this.request = request;
            this.listener = listener;

            this.clusterState = clusterService.state();
            nodes = clusterState.nodes();

            clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

            String[] concreteIndices = clusterState.metaData().concreteIndices(request.indices(), false, true);

            for (String index : concreteIndices) {
                clusterState.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, index);
            }

            Map<String, Set<String>> routingMap = clusterState.metaData().resolveSearchRouting(request.routing(), request.indices());

            shardsIts = clusterService.operationRouting().searchShards(clusterState, request.indices(), concreteIndices, request.queryHint(), routingMap, request.preference());
            expectedSuccessfulOps = shardsIts.size();
            // we need to add 1 for non active partition, since we count it in the total!
            expectedTotalOps = shardsIts.totalSizeWith1ForEmpty();

            if (expectedSuccessfulOps == 0) {
                // not search shards to search on...
                throw new SearchPhaseExecutionException("initial", "No indices / shards to search on, requested indices are " + Arrays.toString(request.indices()), buildShardFailures());
            }
        }

        public void start() {
            request.beforeStart();
            // count the local operations, and perform the non local ones
            int localOperations = 0;
            for (final ShardIterator shardIt : shardsIts) {
                final ShardRouting shard = shardIt.firstOrNull();
                if (shard != null) {
                    if (shard.currentNodeId().equals(nodes.localNodeId())) {
                        localOperations++;
                    } else {
                        // do the remote operation here, the localAsync flag is not relevant
                        performFirstPhase(shardIt);
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
                        @Override
                        public void run() {
                            for (final ShardIterator shardIt : shardsIts) {
                                final ShardRouting shard = shardIt.firstOrNull();
                                if (shard != null) {
                                    if (shard.currentNodeId().equals(nodes.localNodeId())) {
                                        performFirstPhase(shardIt);
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
                        final ShardRouting shard = shardIt.firstOrNull();
                        if (shard != null) {
                            if (shard.currentNodeId().equals(nodes.localNodeId())) {
                                if (localAsync) {
                                    threadPool.executor(ThreadPool.Names.SEARCH).execute(new Runnable() {
                                        @Override
                                        public void run() {
                                            performFirstPhase(shardIt);
                                        }
                                    });
                                } else {
                                    performFirstPhase(shardIt);
                                }
                            }
                        }
                    }
                }
            }
        }

        void performFirstPhase(final ShardIterator shardIt) {
            performFirstPhase(shardIt, shardIt.nextOrNull());
        }

        void performFirstPhase(final ShardIterator shardIt, final ShardRouting shard) {
            if (shard == null) {
                // no more active shards... (we should not really get here, but just for safety)
                onFirstPhaseResult(null, shardIt, null);
            } else {
                DiscoveryNode node = nodes.get(shard.currentNodeId());
                if (node == null) {
                    onFirstPhaseResult(shard, shardIt, null);
                } else {
                    String[] filteringAliases = clusterState.metaData().filteringAliases(shard.index(), request.indices());
                    sendExecuteFirstPhase(node, internalSearchRequest(shard, shardsIts.size(), request, filteringAliases, startTime), new SearchServiceListener<FirstResult>() {
                        @Override
                        public void onResult(FirstResult result) {
                            onFirstPhaseResult(shard, result, shardIt);
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            onFirstPhaseResult(shard, shardIt, t);
                        }
                    });
                }
            }
        }

        void onFirstPhaseResult(ShardRouting shard, FirstResult result, ShardIterator shardIt) {
            result.shardTarget(new SearchShardTarget(shard.currentNodeId(), shard.index(), shard.id()));
            processFirstPhaseResult(shard, result);
            // increment all the "future" shards to update the total ops since we some may work and some may not...
            // and when that happens, we break on total ops, so we must maintain them
            int xTotalOps = totalOps.addAndGet(shardIt.remaining() + 1);
            successulOps.incrementAndGet();
            if (xTotalOps == expectedTotalOps) {
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

        void onFirstPhaseResult(@Nullable ShardRouting shard, final ShardIterator shardIt, Throwable t) {
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
                    addShardFailure(new ShardSearchFailure("No active shards", new SearchShardTarget(null, shardIt.shardId().index().name(), shardIt.shardId().id())));
                } else {
                    addShardFailure(new ShardSearchFailure(t));
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
                ShardRouting nextShard = shardIt.nextOrNull();
                if (nextShard != null) {
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
                    performFirstPhase(shardIt, nextShard);
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
                        addShardFailure(new ShardSearchFailure("No active shards", new SearchShardTarget(null, shardIt.shardId().index().name(), shardIt.shardId().id())));
                    } else {
                        addShardFailure(new ShardSearchFailure(t));
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

        protected final ShardSearchFailure[] buildShardFailures() {
            LinkedTransferQueue<ShardSearchFailure> localFailures = shardFailures;
            if (localFailures == null) {
                return ShardSearchFailure.EMPTY_ARRAY;
            }
            return localFailures.toArray(ShardSearchFailure.EMPTY_ARRAY);
        }

        // we do our best to return the shard failures, but its ok if its not fully concurrently safe
        // we simply try and return as much as possible
        protected final void addShardFailure(ShardSearchFailure failure) {
            if (shardFailures == null) {
                shardFailures = new LinkedTransferQueue<ShardSearchFailure>();
            }
            shardFailures.add(failure);
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
