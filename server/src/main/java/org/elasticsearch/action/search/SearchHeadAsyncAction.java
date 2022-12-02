/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.transport.Transport;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

public class SearchHeadAsyncAction extends AbstractSearchAsyncAction<SearchPhaseResult> {

    private record Phase(int shardIndex, SearchShardIterator shardIt, SearchShardTarget shard) {}

    private static final Logger logger = LogManager.getLogger(SearchHeadAsyncAction.class);

    private final SearchProgressListener progressListener;

    private final int numberOfShards;

    private ArrayDeque<Phase> queuedPhases;

    private final AtomicInteger runningPhases = new AtomicInteger();

    private final AtomicLong totalHits = new AtomicLong();

    private static final int CONCURRENT_PHASES = 5;

    private static final long DEFAULT_HEAD_AGGREGATIONS = 10_000;
    private final long size;

    SearchHeadAsyncAction(
        final Logger logger,
        final SearchTransportService searchTransportService,
        final BiFunction<String, String, Transport.Connection> nodeIdToConnection,
        final Map<String, AliasFilter> aliasFilter,
        final Map<String, Float> concreteIndexBoosts,
        final Executor executor,
        final QueryPhaseResultConsumer resultConsumer,
        final SearchRequest request,
        final ActionListener<SearchResponse> listener,
        final GroupShardsIterator<SearchShardIterator> shardsIts,
        final TransportSearchAction.SearchTimeProvider timeProvider,
        ClusterState clusterState,
        SearchTask task,
        SearchResponse.Clusters clusters
    ) {
        super(
            "preview",
            logger,
            searchTransportService,
            nodeIdToConnection,
            aliasFilter,
            concreteIndexBoosts,
            executor,
            request,
            listener,
            shardsIts,
            timeProvider,
            clusterState,
            task,
            resultConsumer,
            request.getMaxConcurrentShardRequests(),
            clusters
        );
        // don't allow this, if partial results are turned off

        this.progressListener = task.getProgressListener();

        // total number of shards
        this.numberOfShards = shardsIts.totalSizeWith1ForEmpty();
        this.size = (request.source().aggregations() == null || request.source().aggregations().count() == 0)
            ? request.source().size()
            : DEFAULT_HEAD_AGGREGATIONS;

        // todo: depending on size, we should guess how many shards we want to query at once
        if (numberOfShards > CONCURRENT_PHASES) {
            this.queuedPhases = new ArrayDeque<>();
        }

        // register the release of the query consumer to free up the circuit breaker memory
        // at the end of the search
        addReleasable(resultConsumer);

        if (progressListener != SearchProgressListener.NOOP) {
            notifyListShards(progressListener, clusters, request.source());
        }
    }

    protected void executePhaseOnShard(
        final SearchShardIterator shardIt,
        final SearchShardTarget shard,
        final SearchActionListener<SearchPhaseResult> listener
    ) {
        ShardSearchRequest request = super.buildShardSearchRequest(shardIt, listener.requestIndex);
        getSearchTransport().sendExecuteQuery(getConnection(shard.getClusterAlias(), shard.getNodeId()), request, getTask(), listener);
    }

    @Override
    protected SearchPhase getNextPhase(SearchPhaseResults<SearchPhaseResult> results, SearchPhaseContext context) {
        return new FetchSearchPhase(results, null, this);
    }

    protected void performPhaseOnShard(final int shardIndex, final SearchShardIterator shardIt, final SearchShardTarget shard) {
        /**
         * Todo's:
         *  - find better selection strategy:
         *    - at random
         *    - based on age (but maybe not the youngest)
         *    - at least once for every cluster (local and remote)
         *    - at most once per node for equal load distribution
         *  - corner cases / problems
         *    - what if the user added a query that selects certain shards, this might cause a long runtime if shards are sorted by age
         *    - skipped shards prevent error reporting
         *    - how to select the number of docs if e.g. aggs are involved
         */
        if (runningPhases.get() >= CONCURRENT_PHASES) {
            queuedPhases.add(new Phase(shardIndex, shardIt, shard));
        } else {
            runningPhases.incrementAndGet();
            super.performPhaseOnShard(shardIndex, shardIt, shard);
        }
    }

    @Override
    protected void onShardResult(SearchPhaseResult result, SearchShardIterator shardIt) {

        // todo: depending on the order of incoming results this approach is not deterministic
        // to make it deterministic, we would need to wait for all pending results until we make this check and then
        // fire another CONCURRENT_PHASES
        runningPhases.decrementAndGet();

        // todo: if aggs are involved, should this check the number of buckets?
        if (totalHits.addAndGet(result.queryResult().getTotalHits().value) > size) {
            skipRemainingShards();
        } else {
            queryOneMoreShard();
        }

        super.onShardResult(result, shardIt);
    }

    private synchronized void queryOneMoreShard() {
        if (queuedPhases == null || queuedPhases.isEmpty()) {
            return;
        }

        Phase phase = queuedPhases.pop();
        runningPhases.incrementAndGet();
        super.performPhaseOnShard(phase.shardIndex, phase.shardIt, phase.shard);
    }

    private synchronized void skipRemainingShards() {
        if (queuedPhases == null) {
            return;
        }

        while (queuedPhases.isEmpty() == false) {
            Phase phase = queuedPhases.pop();
            super.skipShard(phase.shardIt);
        }
    }
}
