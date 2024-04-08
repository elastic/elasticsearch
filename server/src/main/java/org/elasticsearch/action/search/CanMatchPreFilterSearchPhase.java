/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.CoordinatorRewriteContext;
import org.elasticsearch.index.query.CoordinatorRewriteContextProvider;
import org.elasticsearch.search.CanMatchShardResponse;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.MinAndMax;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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
final class CanMatchPreFilterSearchPhase extends SearchPhase {

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

    private final CanMatchSearchPhaseResults results;
    private final CoordinatorRewriteContextProvider coordinatorRewriteContextProvider;

    CanMatchPreFilterSearchPhase(
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
        super("can_match");
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
        this.shardItIndexMap = new HashMap<>();
        results = new CanMatchSearchPhaseResults(shardsIts.size());

        // we compute the shard index based on the natural order of the shards
        // that participate in the search request. This means that this number is
        // consistent between two requests that target the same shards.
        List<SearchShardIterator> naturalOrder = new ArrayList<>();
        shardsIts.iterator().forEachRemaining(naturalOrder::add);
        CollectionUtil.timSort(naturalOrder);
        for (int i = 0; i < naturalOrder.size(); i++) {
            shardItIndexMap.put(naturalOrder.get(i), i);
        }
    }

    private static boolean assertSearchCoordinationThread() {
        return ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH_COORDINATION);
    }

    @Override
    public void run() {
        assert assertSearchCoordinationThread();
        checkNoMissingShards();
        Version version = request.minCompatibleShardNode();
        if (version != null && Version.CURRENT.minimumCompatibilityVersion().equals(version) == false) {
            if (checkMinimumVersion(shardsIts) == false) {
                throw new VersionMismatchException(
                    "One of the shards is incompatible with the required minimum version [{}]",
                    request.minCompatibleShardNode()
                );
            }
        }
        runCoordinatorRewritePhase();
    }

    // tries to pre-filter shards based on information that's available to the coordinator
    // without having to reach out to the actual shards
    private void runCoordinatorRewritePhase() {
        // TODO: the index filter (i.e, `_index:patten`) should be prefiltered on the coordinator
        assert assertSearchCoordinationThread();
        final List<SearchShardIterator> matchedShardLevelRequests = new ArrayList<>();
        for (SearchShardIterator searchShardIterator : shardsIts) {
            final CanMatchNodeRequest canMatchNodeRequest = new CanMatchNodeRequest(
                request,
                searchShardIterator.getOriginalIndices().indicesOptions(),
                Collections.emptyList(),
                getNumShards(),
                timeProvider.absoluteStartMillis(),
                searchShardIterator.getClusterAlias()
            );
            final ShardSearchRequest request = canMatchNodeRequest.createShardSearchRequest(buildShardLevelRequest(searchShardIterator));
            if (searchShardIterator.prefiltered()) {
                consumeResult(searchShardIterator.skip() == false, request);
                continue;
            }
            boolean canMatch = true;
            CoordinatorRewriteContext coordinatorRewriteContext = coordinatorRewriteContextProvider.getCoordinatorRewriteContext(
                request.shardId().getIndex()
            );
            if (coordinatorRewriteContext != null) {
                try {
                    canMatch = SearchService.queryStillMatchesAfterRewrite(request, coordinatorRewriteContext);
                } catch (Exception e) {
                    // treat as if shard is still a potential match
                }
            }
            if (canMatch) {
                matchedShardLevelRequests.add(searchShardIterator);
            } else {
                consumeResult(false, request);
            }
        }
        if (matchedShardLevelRequests.isEmpty()) {
            finishPhase();
        } else {
            new Round(new GroupShardsIterator<>(matchedShardLevelRequests)).run();
        }
    }

    private void consumeResult(boolean canMatch, ShardSearchRequest request) {
        CanMatchShardResponse result = new CanMatchShardResponse(canMatch, null);
        result.setShardIndex(request.shardRequestIndex());
        results.consumeResult(result, () -> {});
    }

    private void checkNoMissingShards() {
        assert assertSearchCoordinationThread();
        doCheckNoMissingShards(getName(), request, shardsIts);
    }

    private Map<SendingTarget, List<SearchShardIterator>> groupByNode(GroupShardsIterator<SearchShardIterator> shards) {
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
                requests.computeIfAbsent(new SendingTarget(null, null), t -> new ArrayList<>()).add(shardRoutings);
            }
        }
        return requests;
    }

    /**
     * Sending can-match requests is round-based and grouped per target node.
     * If there are failures during a round, there will be a follow-up round
     * to retry on other available shard copies.
     */
    class Round extends AbstractRunnable {
        private final GroupShardsIterator<SearchShardIterator> shards;
        private final CountDown countDown;
        private final AtomicReferenceArray<Exception> failedResponses;

        Round(GroupShardsIterator<SearchShardIterator> shards) {
            this.shards = shards;
            this.countDown = new CountDown(shards.size());
            this.failedResponses = new AtomicReferenceArray<>(shardsIts.size());
        }

        @Override
        protected void doRun() {
            assert assertSearchCoordinationThread();
            final Map<SendingTarget, List<SearchShardIterator>> requests = groupByNode(shards);

            for (Map.Entry<SendingTarget, List<SearchShardIterator>> entry : requests.entrySet()) {
                CanMatchNodeRequest canMatchNodeRequest = createCanMatchRequest(entry);
                List<CanMatchNodeRequest.Shard> shardLevelRequests = canMatchNodeRequest.getShardLevelRequests();

                if (entry.getKey().nodeId == null) {
                    // no target node: just mark the requests as failed
                    for (CanMatchNodeRequest.Shard shard : shardLevelRequests) {
                        onOperationFailed(shard.getShardRequestIndex(), null);
                    }
                    continue;
                }

                try {
                    searchTransportService.sendCanMatch(getConnection(entry.getKey()), canMatchNodeRequest, task, new ActionListener<>() {
                        @Override
                        public void onResponse(CanMatchNodeResponse canMatchNodeResponse) {
                            assert canMatchNodeResponse.getResponses().size() == canMatchNodeRequest.getShardLevelRequests().size();
                            for (int i = 0; i < canMatchNodeResponse.getResponses().size(); i++) {
                                CanMatchNodeResponse.ResponseOrFailure response = canMatchNodeResponse.getResponses().get(i);
                                if (response.getResponse() != null) {
                                    CanMatchShardResponse shardResponse = response.getResponse();
                                    shardResponse.setShardIndex(shardLevelRequests.get(i).getShardRequestIndex());
                                    onOperation(shardResponse.getShardIndex(), shardResponse);
                                } else {
                                    Exception failure = response.getException();
                                    assert failure != null;
                                    onOperationFailed(shardLevelRequests.get(i).getShardRequestIndex(), failure);
                                }
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            for (CanMatchNodeRequest.Shard shard : shardLevelRequests) {
                                onOperationFailed(shard.getShardRequestIndex(), e);
                            }
                        }
                    });
                } catch (Exception e) {
                    for (CanMatchNodeRequest.Shard shard : shardLevelRequests) {
                        onOperationFailed(shard.getShardRequestIndex(), e);
                    }
                }
            }
        }

        private void onOperation(int idx, CanMatchShardResponse response) {
            failedResponses.set(idx, null);
            results.consumeResult(response, () -> {
                if (countDown.countDown()) {
                    finishRound();
                }
            });
        }

        private void onOperationFailed(int idx, Exception e) {
            failedResponses.set(idx, e);
            results.consumeShardFailure(idx);
            if (countDown.countDown()) {
                finishRound();
            }
        }

        private void finishRound() {
            List<SearchShardIterator> remainingShards = new ArrayList<>();
            for (SearchShardIterator ssi : shards) {
                int shardIndex = shardItIndexMap.get(ssi);
                Exception failedResponse = failedResponses.get(shardIndex);
                if (failedResponse != null) {
                    remainingShards.add(ssi);
                }
            }
            if (remainingShards.isEmpty()) {
                finishPhase();
            } else {
                // trigger another round, forcing execution
                executor.execute(new Round(new GroupShardsIterator<>(remainingShards)) {
                    @Override
                    public boolean isForceExecution() {
                        return true;
                    }
                });
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug(() -> format("Failed to execute [%s] while running [%s] phase", request, getName()), e);
            }
            onPhaseFailure("round", e);
        }
    }

    private record SendingTarget(@Nullable String clusterAlias, @Nullable String nodeId) {}

    private CanMatchNodeRequest createCanMatchRequest(Map.Entry<SendingTarget, List<SearchShardIterator>> entry) {
        final SearchShardIterator first = entry.getValue().get(0);
        final List<CanMatchNodeRequest.Shard> shardLevelRequests = entry.getValue()
            .stream()
            .map(this::buildShardLevelRequest)
            .collect(Collectors.toCollection(ArrayList::new));
        assert entry.getValue().stream().allMatch(Objects::nonNull);
        assert entry.getValue()
            .stream()
            .allMatch(ssi -> Objects.equals(ssi.getOriginalIndices().indicesOptions(), first.getOriginalIndices().indicesOptions()));
        assert entry.getValue().stream().allMatch(ssi -> Objects.equals(ssi.getClusterAlias(), first.getClusterAlias()));
        return new CanMatchNodeRequest(
            request,
            first.getOriginalIndices().indicesOptions(),
            shardLevelRequests,
            getNumShards(),
            timeProvider.absoluteStartMillis(),
            first.getClusterAlias()
        );
    }

    private void finishPhase() {
        listener.onResponse(getIterator(results, shardsIts));
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

    private boolean checkMinimumVersion(GroupShardsIterator<SearchShardIterator> shardsIts) {
        for (SearchShardIterator it : shardsIts) {
            if (it.getTargetNodeIds().isEmpty() == false) {
                boolean isCompatible = it.getTargetNodeIds().stream().anyMatch(nodeId -> {
                    Transport.Connection conn = getConnection(new SendingTarget(it.getClusterAlias(), nodeId));
                    return conn == null || conn.getNode().getVersion().onOrAfter(request.minCompatibleShardNode());
                });
                if (isCompatible == false) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public void start() {
        if (getNumShards() == 0) {
            finishPhase();
            return;
        }
        // Note that the search is failed when this task is rejected by the executor
        executor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug(() -> format("Failed to execute [%s] while running [%s] phase", request, getName()), e);
                }
                onPhaseFailure("start", e);
            }

            @Override
            protected void doRun() {
                CanMatchPreFilterSearchPhase.this.run();
            }
        });
    }

    public void onPhaseFailure(String msg, Exception cause) {
        listener.onFailure(new SearchPhaseExecutionException(getName(), msg, cause, ShardSearchFailure.EMPTY_ARRAY));
    }

    public Transport.Connection getConnection(SendingTarget sendingTarget) {
        Transport.Connection conn = nodeIdToConnection.apply(sendingTarget.clusterAlias, sendingTarget.nodeId);
        Version minVersion = request.minCompatibleShardNode();
        if (minVersion != null && conn != null && conn.getNode().getVersion().before(minVersion)) {
            throw new VersionMismatchException("One of the shards is incompatible with the required minimum version [{}]", minVersion);
        }
        return conn;
    }

    private int getNumShards() {
        return shardsIts.size();
    }

    private static final class CanMatchSearchPhaseResults extends SearchPhaseResults<CanMatchShardResponse> {
        private final FixedBitSet possibleMatches;
        private final MinAndMax<?>[] minAndMaxes;
        private int numPossibleMatches;

        CanMatchSearchPhaseResults(int size) {
            super(size);
            possibleMatches = new FixedBitSet(size);
            minAndMaxes = new MinAndMax<?>[size];
        }

        @Override
        void consumeResult(CanMatchShardResponse result, Runnable next) {
            try {
                consumeResult(result.getShardIndex(), result.canMatch(), result.estimatedMinAndMax());
            } finally {
                next.run();
            }
        }

        @Override
        boolean hasResult(int shardIndex) {
            return false; // unneeded
        }

        @Override
        void consumeShardFailure(int shardIndex) {
            // we have to carry over shard failures in order to account for them in the response.
            consumeResult(shardIndex, true, null);
        }

        synchronized void consumeResult(int shardIndex, boolean canMatch, MinAndMax<?> minAndMax) {
            if (canMatch) {
                possibleMatches.set(shardIndex);
                numPossibleMatches++;
            }
            minAndMaxes[shardIndex] = minAndMax;
        }

        synchronized int getNumPossibleMatches() {
            return numPossibleMatches;
        }

        synchronized FixedBitSet getPossibleMatches() {
            return possibleMatches;
        }

        @Override
        Stream<CanMatchShardResponse> getSuccessfulResults() {
            return Stream.empty();
        }

        @Override
        public void close() {}
    }

    private GroupShardsIterator<SearchShardIterator> getIterator(
        CanMatchSearchPhaseResults results,
        GroupShardsIterator<SearchShardIterator> shardsIts
    ) {
        int cardinality = results.getNumPossibleMatches();
        FixedBitSet possibleMatches = results.getPossibleMatches();
        // TODO: pick the local shard when possible
        if (requireAtLeastOneMatch && cardinality == 0) {
            // this is a special case where we have no hit but we need to get at least one search response in order
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
            possibleMatches.set(shardIndexToQuery);
        }
        SearchSourceBuilder source = request.source();
        int i = 0;
        for (SearchShardIterator iter : shardsIts) {
            iter.reset();
            boolean match = possibleMatches.get(i++);
            if (match) {
                assert iter.skip() == false;
            } else {
                iter.skip(true);
            }
        }
        if (shouldSortShards(results.minAndMaxes) == false) {
            return shardsIts;
        }
        FieldSortBuilder fieldSort = FieldSortBuilder.getPrimaryFieldSortOrNull(source);
        return new GroupShardsIterator<>(sortShards(shardsIts, results.minAndMaxes, fieldSort.order()));
    }

    private static List<SearchShardIterator> sortShards(
        GroupShardsIterator<SearchShardIterator> shardsIts,
        MinAndMax<?>[] minAndMaxes,
        SortOrder order
    ) {
        return IntStream.range(0, shardsIts.size())
            .boxed()
            .sorted(shardComparator(shardsIts, minAndMaxes, order))
            .map(shardsIts::get)
            .toList();
    }

    private static boolean shouldSortShards(MinAndMax<?>[] minAndMaxes) {
        Class<?> clazz = null;
        for (MinAndMax<?> minAndMax : minAndMaxes) {
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

    private static Comparator<Integer> shardComparator(
        GroupShardsIterator<SearchShardIterator> shardsIts,
        MinAndMax<?>[] minAndMaxes,
        SortOrder order
    ) {
        final Comparator<Integer> comparator = Comparator.comparing(
            index -> minAndMaxes[index],
            forciblyCast(MinAndMax.getComparator(order))
        );

        return comparator.thenComparing(shardsIts::get);
    }

}
