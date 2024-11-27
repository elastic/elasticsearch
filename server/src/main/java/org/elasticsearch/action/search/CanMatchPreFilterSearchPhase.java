/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.CoordinatorRewriteContext;
import org.elasticsearch.index.query.CoordinatorRewriteContextProvider;
import org.elasticsearch.search.CanMatchShardResponse;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.MinAndMax;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiFunction;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.core.Types.forciblyCast;

/**
 * This search phase can be used as an initial search phase to pre-filter search shards based on query rewriting.
 * The queries are rewritten against the shards and based on the rewrite result shards might be able to be excluded
 * from the search. The extra round trip to the search shards is very cheap and is not subject to rejections
 * which allows to fan out to more shards at the same time without running into rejections even if we are hitting a
 * large portion of the clusters indices.
 * This phase can also be used to pre-sort shards based on min/max values in each shard of the provided primary sort.
 * When the query primary sort is perform on a field, this phase extracts the min/max value in each shard and
 * sort them according to the provided order. This can be useful for instance to ensure that shards that contain recent
 * data are executed first when sorting by descending timestamp.
 */
final class CanMatchPreFilterSearchPhase {

    /** Value to use in {@link #res} for shards that match but where no {@link MinAndMax} value is available*/
    private static final Object TRUE_SENTINEL = new Object();

    private final Logger logger;
    private final SearchRequest request;
    private final GroupShardsIterator<SearchShardIterator> shardsIts;
    private final ActionListener<GroupShardsIterator<SearchShardIterator>> listener;
    private final TransportSearchAction.SearchTimeProvider timeProvider;
    private final BiFunction<String, String, Transport.Connection> nodeIdToConnection;
    private final SearchTransportService searchTransportService;
    private final Map<SearchShardIterator, Integer> shardItIndexMap;
    private final Map<String, Float> concreteIndexBoosts;
    private final Map<String, AliasFilter> aliasFilter;
    private final SearchTask task;
    private final Executor executor;
    private final boolean requireAtLeastOneMatch;

    private final CoordinatorRewriteContextProvider coordinatorRewriteContextProvider;

    private final AtomicReferenceArray<Object> res;

    private CanMatchPreFilterSearchPhase(
        Logger logger,
        SearchTransportService searchTransportService,
        BiFunction<String, String, Transport.Connection> nodeIdToConnection,
        Map<String, AliasFilter> aliasFilter,
        Map<String, Float> concreteIndexBoosts,
        Executor executor,
        SearchRequest request,
        GroupShardsIterator<SearchShardIterator> shardsIts,
        TransportSearchAction.SearchTimeProvider timeProvider,
        SearchTask task,
        boolean requireAtLeastOneMatch,
        CoordinatorRewriteContextProvider coordinatorRewriteContextProvider,
        ActionListener<GroupShardsIterator<SearchShardIterator>> listener
    ) {
        this.logger = logger;
        this.searchTransportService = searchTransportService;
        this.nodeIdToConnection = nodeIdToConnection;
        this.request = request;
        this.listener = listener;
        this.shardsIts = shardsIts;
        this.timeProvider = timeProvider;
        this.concreteIndexBoosts = concreteIndexBoosts;
        this.aliasFilter = aliasFilter;
        this.task = task;
        this.requireAtLeastOneMatch = requireAtLeastOneMatch;
        this.coordinatorRewriteContextProvider = coordinatorRewriteContextProvider;
        this.executor = executor;
        int size = shardsIts.size();
        res = new AtomicReferenceArray<>(size);

        // we compute the shard index based on the natural order of the shards
        // that participate in the search request. This means that this number is
        // consistent between two requests that target the same shards.
        final SearchShardIterator[] naturalOrder = new SearchShardIterator[size];
        int i = 0;
        for (SearchShardIterator shardsIt : shardsIts) {
            naturalOrder[i++] = shardsIt;
        }
        Arrays.sort(naturalOrder);
        final Map<SearchShardIterator, Integer> shardItIndexMap = Maps.newHashMapWithExpectedSize(naturalOrder.length);
        for (int j = 0; j < naturalOrder.length; j++) {
            shardItIndexMap.put(naturalOrder[j], j);
        }
        this.shardItIndexMap = shardItIndexMap;
    }

    public static void execute(
        Logger logger,
        SearchTransportService searchTransportService,
        BiFunction<String, String, Transport.Connection> nodeIdToConnection,
        Map<String, AliasFilter> aliasFilter,
        Map<String, Float> concreteIndexBoosts,
        Executor executor,
        SearchRequest request,
        GroupShardsIterator<SearchShardIterator> shardsIts,
        TransportSearchAction.SearchTimeProvider timeProvider,
        SearchTask task,
        boolean requireAtLeastOneMatch,
        CoordinatorRewriteContextProvider coordinatorRewriteContextProvider,
        ActionListener<GroupShardsIterator<SearchShardIterator>> listener
    ) {
        if (shardsIts.size() == 0) {
            listener.onResponse(shardsIts);
            return;
        }
        new CanMatchPreFilterSearchPhase(
            logger,
            searchTransportService,
            nodeIdToConnection,
            aliasFilter,
            concreteIndexBoosts,
            executor,
            request,
            shardsIts,
            timeProvider,
            task,
            requireAtLeastOneMatch,
            coordinatorRewriteContextProvider,
            listener
        ).start();
    }

    private static boolean assertSearchCoordinationThread() {
        return ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH_COORDINATION);
    }

    // tries to pre-filter shards based on information that's available to the coordinator
    // without having to reach out to the actual shards
    private void runCoordinatorRewritePhase() {
        // TODO: the index filter (i.e, `_index:patten`) should be prefiltered on the coordinator
        assert assertSearchCoordinationThread();
        final List<SearchShardIterator> matchedShardLevelRequests = new ArrayList<>();
        for (SearchShardIterator searchShardIterator : shardsIts) {
            if (searchShardIterator.prefiltered()) {
                if (searchShardIterator.skip() == false) {
                    res.set(shardItIndexMap.get(searchShardIterator), TRUE_SENTINEL);
                }
                continue;
            }
            boolean canMatch = true;
            CoordinatorRewriteContext coordinatorRewriteContext = coordinatorRewriteContextProvider.getCoordinatorRewriteContext(
                searchShardIterator.shardId().getIndex()
            );
            if (coordinatorRewriteContext != null) {
                try {
                    canMatch = SearchService.queryStillMatchesAfterRewrite(
                        new CanMatchNodeRequest(
                            request,
                            searchShardIterator.getOriginalIndices().indicesOptions(),
                            Collections.emptyList(),
                            shardsIts.size(),
                            timeProvider.absoluteStartMillis(),
                            searchShardIterator.getClusterAlias()
                        ).createShardSearchRequest(buildShardLevelRequest(searchShardIterator)),
                        coordinatorRewriteContext
                    );
                } catch (Exception e) {
                    // treat as if shard is still a potential match
                }
            }
            if (canMatch) {
                matchedShardLevelRequests.add(searchShardIterator);
            }
        }
        if (matchedShardLevelRequests.isEmpty()) {
            finishPhase();
        } else {
            GroupShardsIterator<SearchShardIterator> matchingShards = new GroupShardsIterator<>(matchedShardLevelRequests);
            // verify missing shards only for the shards that we hit for the query
            SearchPhase.doCheckNoMissingShards("can_match", request, matchingShards, SearchPhase::makeMissingShardsError);
            new Round(matchingShards).run();
        }
    }

    /**
     * Sending can-match requests is round-based and grouped per target node.
     * If there are failures during a round, there will be a follow-up round
     * to retry on other available shard copies.
     */
    private final class Round extends AbstractRunnable {
        private final GroupShardsIterator<SearchShardIterator> shards;
        private final CountDown countDown;

        Round(GroupShardsIterator<SearchShardIterator> shards) {
            this.shards = shards;
            this.countDown = new CountDown(shards.size());
        }

        @Override
        protected void doRun() {
            assert assertSearchCoordinationThread();
            Map<SendingTarget, List<SearchShardIterator>> requests = new HashMap<>();
            for (int i = 0; i < shards.size(); i++) {
                final SearchShardIterator shardRoutings = shards.get(i);
                assert shardRoutings.skip() == false;
                assert shardItIndexMap.containsKey(shardRoutings);
                SearchShardTarget target = shardRoutings.nextOrNull();
                if (target != null) {
                    requests.computeIfAbsent(new SendingTarget(target.getClusterAlias(), target.getNodeId()), t -> new ArrayList<>())
                        .add(shardRoutings);
                } else {
                    onOperationResult(shardItIndexMap.get(shardRoutings), TRUE_SENTINEL);
                }
            }

            for (Map.Entry<SendingTarget, List<SearchShardIterator>> entry : requests.entrySet()) {
                CanMatchNodeRequest canMatchNodeRequest = createCanMatchRequest(entry.getValue());
                List<CanMatchNodeRequest.Shard> shardLevelRequests = canMatchNodeRequest.getShardLevelRequests();
                var sendingTarget = entry.getKey();
                var listener = new ActionListener<CanMatchNodeResponse>() {
                    @Override
                    public void onResponse(CanMatchNodeResponse canMatchNodeResponse) {
                        var responses = canMatchNodeResponse.getResponses();
                        assert responses.size() == shardLevelRequests.size();
                        for (int i = 0; i < responses.size(); i++) {
                            CanMatchNodeResponse.ResponseOrFailure response = responses.get(i);
                            CanMatchShardResponse shardResponse = response.getResponse();
                            final Object shardResult;
                            if (shardResponse != null) {
                                shardResult = shardResponse.canMatch()
                                    ? Objects.requireNonNullElse(shardResponse.estimatedMinAndMax(), TRUE_SENTINEL)
                                    : null;
                            } else {
                                Exception failure = response.getException();
                                assert failure != null;
                                shardResult = failure;
                            }
                            onOperationResult(shardLevelRequests.get(i).getShardRequestIndex(), shardResult);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        for (CanMatchNodeRequest.Shard shard : shardLevelRequests) {
                            onOperationResult(shard.getShardRequestIndex(), e);
                        }
                    }
                };
                try {
                    searchTransportService.sendCanMatch(
                        nodeIdToConnection.apply(sendingTarget.clusterAlias, sendingTarget.nodeId),
                        canMatchNodeRequest,
                        task,
                        listener
                    );
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }
        }

        @Override
        public boolean isForceExecution() {
            return true;
        }

        private void onOperationResult(int idx, Object result) {
            res.set(idx, result);
            if (countDown.countDown()) {
                finishRound();
            }
        }

        private void finishRound() {
            List<SearchShardIterator> remainingShards = new ArrayList<>();
            for (SearchShardIterator ssi : shards) {
                int idx = shardItIndexMap.get(ssi);
                Exception failedResponse = res.get(idx) instanceof Exception e ? e : null;
                if (failedResponse != null) {
                    remainingShards.add(ssi);
                }
            }
            if (remainingShards.isEmpty()) {
                finishPhase();
            } else {
                // trigger another round, forcing execution
                executor.execute(new Round(new GroupShardsIterator<>(remainingShards)));
            }
        }

        @Override
        public void onFailure(Exception e) {
            onPhaseFailure("round", e);
        }
    }

    private record SendingTarget(@Nullable String clusterAlias, @Nullable String nodeId) {}

    private CanMatchNodeRequest createCanMatchRequest(List<SearchShardIterator> iters) {
        final SearchShardIterator first = iters.get(0);
        assert iters.stream()
            .allMatch(
                ssi -> ssi != null
                    && Objects.equals(ssi.getOriginalIndices().indicesOptions(), first.getOriginalIndices().indicesOptions())
                    && Objects.equals(ssi.getClusterAlias(), first.getClusterAlias())
            );
        return new CanMatchNodeRequest(
            request,
            first.getOriginalIndices().indicesOptions(),
            iters.stream().map(this::buildShardLevelRequest).toList(),
            shardsIts.size(),
            timeProvider.absoluteStartMillis(),
            first.getClusterAlias()
        );
    }

    private void finishPhase() {
        listener.onResponse(getIterator());
    }

    private static final float DEFAULT_INDEX_BOOST = 1.0f;

    public CanMatchNodeRequest.Shard buildShardLevelRequest(SearchShardIterator shardIt) {
        AliasFilter filter = aliasFilter.get(shardIt.shardId().getIndex().getUUID());
        assert filter != null;
        float indexBoost = concreteIndexBoosts.getOrDefault(shardIt.shardId().getIndex().getUUID(), DEFAULT_INDEX_BOOST);
        int shardRequestIndex = shardItIndexMap.get(shardIt);
        return new CanMatchNodeRequest.Shard(
            shardIt.getOriginalIndices().indices(),
            shardIt.shardId(),
            shardRequestIndex,
            filter,
            indexBoost,
            shardIt.getSearchContextId(),
            shardIt.getSearchContextKeepAlive(),
            ShardSearchRequest.computeWaitForCheckpoint(request.getWaitForCheckpoints(), shardIt.shardId(), shardRequestIndex)
        );
    }

    private void start() {
        // Note that the search is failed when this task is rejected by the executor
        executor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                onPhaseFailure("start", e);
            }

            @Override
            protected void doRun() {
                assert assertSearchCoordinationThread();
                runCoordinatorRewritePhase();
            }
        });
    }

    private void onPhaseFailure(String msg, Exception cause) {
        if (logger.isDebugEnabled()) {
            logger.debug(() -> format("Failed to execute [%s] while running [can_match] phase", request), cause);
        }
        listener.onFailure(new SearchPhaseExecutionException("can_match", msg, cause, ShardSearchFailure.EMPTY_ARRAY));
    }

    private GroupShardsIterator<SearchShardIterator> getIterator() {
        // TODO: pick the local shard when possible
        if (requireAtLeastOneMatch) {
            boolean isEmpty = true;
            for (int i = 0, n = res.length(); i < n; i++) {
                if (res.get(i) != null) {
                    isEmpty = false;
                    break;
                }
            }
            if (isEmpty) {
                // this is a special case where we have no hit, but we need to get at least one search response in order
                // to produce a valid search result with all the aggs etc.
                // Since it's possible that some of the shards that we're skipping are
                // unavailable, we would try to query the node that at least has some
                // shards available in order to produce a valid search result.
                int shardIndexToQuery = 0;
                for (int i = 0; i < shardsIts.size(); i++) {
                    SearchShardIterator it = shardsIts.get(i);
                    if (it.size() > 0) {
                        shardIndexToQuery = i;
                        it.skip(false); // un-skip which is needed when all the remote shards were skipped by the remote can_match
                        break;
                    }
                }
                res.set(shardIndexToQuery, TRUE_SENTINEL);
            }
        }
        int i = 0;
        for (SearchShardIterator iter : shardsIts) {
            iter.reset();
            if (res.get(i++) != null) {
                assert iter.skip() == false;
            } else {
                iter.skip(true);
            }
        }
        return shouldSortShards(res)
            ? new GroupShardsIterator<>(sortShards(shardsIts, res, FieldSortBuilder.getPrimaryFieldSortOrNull(request.source()).order()))
            : shardsIts;
    }

    private static MinAndMax<?> getMinAndMax(AtomicReferenceArray<Object> res, int shardIndex) {
        return res.get(shardIndex) instanceof MinAndMax<?> m ? m : null;
    }

    private static List<SearchShardIterator> sortShards(
        GroupShardsIterator<SearchShardIterator> shardsIts,
        AtomicReferenceArray<Object> results,
        SortOrder order
    ) {
        int bound = shardsIts.size();
        Integer[] toSort = new Integer[bound];
        for (int i = 0; i < bound; i++) {
            toSort[i] = i;
        }
        Comparator<? super MinAndMax<?>> keyComparator = forciblyCast(MinAndMax.getComparator(order));
        Arrays.sort(toSort, (idx1, idx2) -> {
            int res = keyComparator.compare(getMinAndMax(results, idx1), getMinAndMax(results, idx2));
            if (res != 0) {
                return res;
            }
            return shardsIts.get(idx1).compareTo(shardsIts.get(idx2));
        });
        List<SearchShardIterator> list = new ArrayList<>(bound);
        for (Integer integer : toSort) {
            list.add(shardsIts.get(integer));
        }
        return list;
    }

    private static boolean shouldSortShards(AtomicReferenceArray<Object> results) {
        Class<?> clazz = null;
        for (int i = 0, n = results.length(); i < n; i++) {
            var minAndMax = getMinAndMax(results, i);
            if (clazz == null) {
                clazz = minAndMax == null ? null : minAndMax.getMin().getClass();
            } else if (minAndMax != null && clazz != minAndMax.getMin().getClass()) {
                // we don't support sort values that mix different types (e.g.: long/double, numeric/keyword).
                // TODO: we could fail the request because there is a high probability
                // that the merging of topdocs will fail later for the same reason ?
                return false;
            }
        }
        return clazz != null;
    }

}
