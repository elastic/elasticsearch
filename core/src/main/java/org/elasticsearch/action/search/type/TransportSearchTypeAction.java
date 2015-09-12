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

package org.elasticsearch.action.search.type;

import com.carrotsearch.hppc.IntArrayList;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.search.ReduceSearchPhaseException;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
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

    public TransportSearchTypeAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                     SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController,
                                     ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, SearchAction.NAME, threadPool, actionFilters, indexNameExpressionResolver);
        this.clusterService = clusterService;
        this.searchService = searchService;
        this.searchPhaseController = searchPhaseController;
    }

    protected abstract class BaseAsyncAction<FirstResult extends SearchPhaseResult> extends AbstractAsyncAction {

        protected final ActionListener<SearchResponse> listener;

        protected final GroupShardsIterator shardsIts;

        protected final SearchRequest request;

        protected final ClusterState clusterState;
        protected final DiscoveryNodes nodes;

        protected final int expectedSuccessfulOps;
        private final int expectedTotalOps;

        protected final AtomicInteger successfulOps = new AtomicInteger();
        private final AtomicInteger totalOps = new AtomicInteger();

        protected final AtomicArray<FirstResult> firstResults;
        private volatile AtomicArray<ShardSearchFailure> shardFailures;
        private final Object shardFailuresMutex = new Object();
        protected volatile ScoreDoc[] sortedShardList;

        protected BaseAsyncAction(SearchRequest request, ActionListener<SearchResponse> listener) {
            this.request = request;
            this.listener = listener;

            this.clusterState = clusterService.state();
            nodes = clusterState.nodes();

            clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

            // TODO: I think startTime() should become part of ActionRequest and that should be used both for index name
            // date math expressions and $now in scripts. This way all apis will deal with now in the same way instead
            // of just for the _search api
            String[] concreteIndices = indexNameExpressionResolver.concreteIndices(clusterState, request.indicesOptions(), startTime(), request.indices());

            for (String index : concreteIndices) {
                clusterState.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, index);
            }

            Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, request.routing(), request.indices());

            shardsIts = clusterService.operationRouting().searchShards(clusterState, concreteIndices, routingMap, request.preference());
            expectedSuccessfulOps = shardsIts.size();
            // we need to add 1 for non active partition, since we count it in the total!
            expectedTotalOps = shardsIts.totalSizeWith1ForEmpty();

            firstResults = new AtomicArray<>(shardsIts.size());
        }

        public void start() {
            if (expectedSuccessfulOps == 0) {
                // no search shards to search on, bail with empty response (it happens with search across _all with no indices around and consistent with broadcast operations)
                listener.onResponse(new SearchResponse(InternalSearchResponse.empty(), null, 0, 0, buildTookInMillis(), ShardSearchFailure.EMPTY_ARRAY));
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
                final DiscoveryNode node = nodes.get(shard.currentNodeId());
                if (node == null) {
                    onFirstPhaseResult(shardIndex, shard, null, shardIt, new NoShardAvailableActionException(shardIt.shardId()));
                } else {
                    String[] filteringAliases = indexNameExpressionResolver.filteringAliases(clusterState, shard.index(), request.indices());
                    sendExecuteFirstPhase(node, internalSearchRequest(shard, shardsIts.size(), request, filteringAliases, startTime()), new ActionListener<FirstResult>() {
                        @Override
                        public void onResponse(FirstResult result) {
                            onFirstPhaseResult(shardIndex, shard, result, shardIt);
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            onFirstPhaseResult(shardIndex, shard, node.id(), shardIt, t);
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
                } catch (Throwable e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(shardIt.shardId() + ": Failed to execute [" + request + "] while moving to second phase", e);
                    }
                    raiseEarlyFailure(new ReduceSearchPhaseException(firstPhaseName(), "", e, buildShardFailures()));
                }
            } else if (xTotalOps > expectedTotalOps) {
                raiseEarlyFailure(new IllegalStateException("unexpected higher total ops [" + xTotalOps + "] compared to expected [" + expectedTotalOps + "]"));
            }
        }

        void onFirstPhaseResult(final int shardIndex, @Nullable ShardRouting shard, @Nullable String nodeId, final ShardIterator shardIt, Throwable t) {
            // we always add the shard failure for a specific shard instance
            // we do make sure to clean it on a successful response from a shard
            SearchShardTarget shardTarget = new SearchShardTarget(nodeId, shardIt.shardId().getIndex(), shardIt.shardId().getId());
            addShardFailure(shardIndex, shardTarget, t);

            if (totalOps.incrementAndGet() == expectedTotalOps) {
                if (logger.isDebugEnabled()) {
                    if (t != null && !TransportActions.isShardNotAvailableException(t)) {
                        if (shard != null) {
                            logger.debug(shard.shortSummary() + ": Failed to execute [" + request + "]", t);
                        } else {
                            logger.debug(shardIt.shardId() + ": Failed to execute [" + request + "]", t);
                        }
                    } else if (logger.isTraceEnabled()) {
                        logger.trace("{}: Failed to execute [{}]", t, shard, request);
                    }
                }
                if (successfulOps.get() == 0) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("All shards failed for phase: [{}]", t, firstPhaseName());
                    }
                    // no successful ops, raise an exception
                    raiseEarlyFailure(new SearchPhaseExecutionException(firstPhaseName(), "all shards failed", buildShardFailures()));
                } else {
                    try {
                        innerMoveToSecondPhase();
                    } catch (Throwable e) {
                        raiseEarlyFailure(new ReduceSearchPhaseException(firstPhaseName(), "", e, buildShardFailures()));
                    }
                }
            } else {
                final ShardRouting nextShard = shardIt.nextOrNull();
                final boolean lastShard = nextShard == null;
                // trace log this exception
                if (logger.isTraceEnabled()) {
                    logger.trace(executionFailureMsg(shard, shardIt, request, lastShard), t);
                }
                if (!lastShard) {
                    try {
                        performFirstPhase(shardIndex, shardIt, nextShard);
                    } catch (Throwable t1) {
                        onFirstPhaseResult(shardIndex, shard, shard.currentNodeId(), shardIt, t1);
                    }
                } else {
                    // no more shards active, add a failure
                    if (logger.isDebugEnabled() && !logger.isTraceEnabled()) { // do not double log this exception
                        if (t != null && !TransportActions.isShardNotAvailableException(t)) {
                            logger.debug(executionFailureMsg(shard, shardIt, request, lastShard), t);
                        }
                    }
                }
            }
        }

        private String executionFailureMsg(@Nullable ShardRouting shard, final ShardIterator shardIt, SearchRequest request, boolean lastShard) {
            if (shard != null) {
                return shard.shortSummary() + ": Failed to execute [" + request + "] lastShard [" + lastShard + "]";
            } else {
                return shardIt.shardId() + ": Failed to execute [" + request + "] lastShard [" + lastShard + "]";
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

        protected final void addShardFailure(final int shardIndex, @Nullable SearchShardTarget shardTarget, Throwable t) {
            // we don't aggregate shard failures on non active shards (but do keep the header counts right)
            if (TransportActions.isShardNotAvailableException(t)) {
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
                shardFailures.set(shardIndex, new ShardSearchFailure(t, shardTarget));
            } else {
                // the failure is already present, try and not override it with an exception that is less meaningless
                // for example, getting illegal shard state
                if (TransportActions.isReadOverrideException(t)) {
                    shardFailures.set(shardIndex, new ShardSearchFailure(t, shardTarget));
                }
            }
        }

        private void raiseEarlyFailure(Throwable t) {
            for (AtomicArray.Entry<FirstResult> entry : firstResults.asList()) {
                try {
                    DiscoveryNode node = nodes.get(entry.value.shardTarget().nodeId());
                    sendReleaseSearchContext(entry.value.id(), node);
                } catch (Throwable t1) {
                    logger.trace("failed to release context", t1);
                }
            }
            listener.onFailure(t);
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
                    final TopDocs topDocs = entry.value.queryResult().queryResult().topDocs();
                    if (topDocs != null && topDocs.scoreDocs.length > 0 // the shard had matches
                            && docIdsToLoad.get(entry.index) == null) { // but none of them made it to the global top docs
                        try {
                            DiscoveryNode node = nodes.get(entry.value.queryResult().shardTarget().nodeId());
                            sendReleaseSearchContext(entry.value.queryResult().id(), node);
                        } catch (Throwable t1) {
                            logger.trace("failed to release context", t1);
                        }
                    }
                }
            }
        }

        protected void sendReleaseSearchContext(long contextId, DiscoveryNode node) {
            if (node != null) {
                searchService.sendFreeContext(node, contextId, request);
            }
        }

        protected ShardFetchSearchRequest createFetchRequest(QuerySearchResult queryResult, AtomicArray.Entry<IntArrayList> entry, ScoreDoc[] lastEmittedDocPerShard) {
            if (lastEmittedDocPerShard != null) {
                ScoreDoc lastEmittedDoc = lastEmittedDocPerShard[entry.index];
                return new ShardFetchSearchRequest(request, queryResult.id(), entry.value, lastEmittedDoc);
            } else {
                return new ShardFetchSearchRequest(request, queryResult.id(), entry.value);
            }
        }

        protected abstract void sendExecuteFirstPhase(DiscoveryNode node, ShardSearchTransportRequest request, ActionListener<FirstResult> listener);

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

                logger.trace("Moving to second phase, based on results from: {} (cluster state version: {})", sb, clusterState.version());
            }
            moveToSecondPhase();
        }

        protected abstract void moveToSecondPhase() throws Exception;

        protected abstract String firstPhaseName();
    }
}
