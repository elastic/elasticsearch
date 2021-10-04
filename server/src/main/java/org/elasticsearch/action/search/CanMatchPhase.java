/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.CoordinatorRewriteContext;
import org.elasticsearch.index.query.CoordinatorRewriteContextProvider;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.MinAndMax;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.Transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.core.Types.forciblyCast;

public class CanMatchPhase extends SearchPhase {

    private final Logger logger;
    private final SearchRequest request;
    private final GroupShardsIterator<SearchShardIterator> shardsIts;
    private final ActionListener<SearchResponse> listener;
    private final SearchResponse.Clusters clusters;
    private final TransportSearchAction.SearchTimeProvider timeProvider;
    private final BiFunction<String, String, Transport.Connection> nodeIdToConnection;
    private final SearchTransportService searchTransportService;
    private final Map<SearchShardIterator, Integer> shardItIndexMap;
    private final Map<String, Float> concreteIndexBoosts;
    private final Map<String, AliasFilter> aliasFilter;
    private final SearchTask task;
    private final Function<GroupShardsIterator<SearchShardIterator>, SearchPhase> phaseFactory;

    private final CanMatchSearchPhaseResults results;
    private final CoordinatorRewriteContextProvider coordinatorRewriteContextProvider;


    public CanMatchPhase(Logger logger, SearchTransportService searchTransportService,
                         BiFunction<String, String, Transport.Connection> nodeIdToConnection,
                         Map<String, AliasFilter> aliasFilter, Map<String, Float> concreteIndexBoosts,
                         Executor executor, SearchRequest request,
                         ActionListener<SearchResponse> listener, GroupShardsIterator<SearchShardIterator> shardsIts,
                         TransportSearchAction.SearchTimeProvider timeProvider, ClusterState clusterState,
                         SearchTask task, Function<GroupShardsIterator<SearchShardIterator>, SearchPhase> phaseFactory,
                         SearchResponse.Clusters clusters, CoordinatorRewriteContextProvider coordinatorRewriteContextProvider) {
        super("can_match");
        this.logger = logger;
        this.searchTransportService = searchTransportService;
        this.nodeIdToConnection = nodeIdToConnection;
        this.request = request;
        this.listener = listener;
        this.shardsIts = shardsIts;
        this.clusters = clusters;
        this.timeProvider = timeProvider;
        this.concreteIndexBoosts = concreteIndexBoosts;
        this.aliasFilter = aliasFilter;
        this.task = task;
        this.phaseFactory = phaseFactory;
        this.coordinatorRewriteContextProvider = coordinatorRewriteContextProvider;
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

    @Override
    public void run() throws IOException {
        if (shardsIts.size() > 0) {
            checkNoMissingShards();
            Version version = request.minCompatibleShardNode();
            if (version != null && Version.CURRENT.minimumCompatibilityVersion().equals(version) == false) {
                if (checkMinimumVersion(shardsIts) == false) {
                    throw new VersionMismatchException("One of the shards is incompatible with the required minimum version [{}]",
                        request.minCompatibleShardNode());
                }
            }

            runCoordinationPhase();
        }
    }

    private void runCoordinationPhase() {
        final List<SearchShardIterator> matchedShardLevelRequests = new ArrayList<>();
        for (SearchShardIterator searchShardIterator : shardsIts) {
            final CanMatchRequest canMatchRequest = new CanMatchRequest(searchShardIterator.getOriginalIndices(), request,
                Collections.singletonList(buildShardLevelRequest(searchShardIterator)), getNumShards(),
                timeProvider.getAbsoluteStartMillis(), searchShardIterator.getClusterAlias());
            List<CanMatchRequest.ShardLevelRequest> shardLevelRequests = canMatchRequest.getShardLevelRequests();
            List<ShardSearchRequest> shardSearchRequests = canMatchRequest.createShardSearchRequests();
            for (int i = 0; i < shardSearchRequests.size(); i++) {
                ShardSearchRequest request = shardSearchRequests.get(i);

                CoordinatorRewriteContext coordinatorRewriteContext =
                    coordinatorRewriteContextProvider.getCoordinatorRewriteContext(request.shardId().getIndex());
                if (coordinatorRewriteContext != null) {
                    boolean canMatch = true;
                    try {
                        canMatch = SearchService.queryStillMatchesAfterRewrite(request, coordinatorRewriteContext);
                    } catch (Exception e) {
                        // ignore
                        // treat as if shard is still a potential match
                    }

                    if (canMatch) {
                        matchedShardLevelRequests.add(searchShardIterator);
                    } else {
                        SearchService.CanMatchResponse result = new SearchService.CanMatchResponse(canMatch, null);
                        result.setShardIndex(request.shardRequestIndex());
                        results.consumeResult(result, () -> {});
                    }
                }
            }
        }

        if (matchedShardLevelRequests.isEmpty() == false) {
            new Round(new GroupShardsIterator<>(matchedShardLevelRequests)).run();
        } else {
            finishHim();
        }
    }

    private void checkNoMissingShards() {
        assert request.allowPartialSearchResults() != null : "SearchRequest missing setting for allowPartialSearchResults";
        if (request.allowPartialSearchResults() == false) {
            final StringBuilder missingShards = new StringBuilder();
            // Fail-fast verification of all shards being available
            for (int index = 0; index < shardsIts.size(); index++) {
                final SearchShardIterator shardRoutings = shardsIts.get(index);
                if (shardRoutings.size() == 0) {
                    if (missingShards.length() > 0) {
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
    }

    private Map<Tuple<String, String>, List<SearchShardIterator>> groupByNode(GroupShardsIterator<SearchShardIterator> shards) {
        Map<Tuple<String, String>, List<SearchShardIterator>> requests = new HashMap<>();
        for (int i = 0; i < shards.size(); i++) {
            final SearchShardIterator shardRoutings = shards.get(i);
            assert shardRoutings.skip() == false;
            assert shardItIndexMap.containsKey(shardRoutings);
            SearchShardTarget target = shardRoutings.nextOrNull();
            if (target != null) {
                requests.computeIfAbsent(Tuple.tuple(target.getClusterAlias(), target.getNodeId()),
                    t -> new ArrayList<>()).add(shardRoutings);
            } else {
                requests.computeIfAbsent(Tuple.tuple(null, null),
                    t -> new ArrayList<>()).add(shardRoutings);
            }
        }
        return requests;
    }

    class Round implements Runnable {
        private final GroupShardsIterator<SearchShardIterator> shards;
        private final AtomicInteger counter = new AtomicInteger();
        private final AtomicReferenceArray<Object> responses;

        Round(GroupShardsIterator<SearchShardIterator> shards) {
            this.shards = shards;
            this.responses = new AtomicReferenceArray<>(shardsIts.size());
        }

        public void start() {
            try {
                run();
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug(new ParameterizedMessage("Failed to execute [{}] while running [{}] phase", request, getName()), e);
                }
                onPhaseFailure(CanMatchPhase.this, "", e);
            }
        }


        @Override
        public void run() {
            final Map<Tuple<String, String>, List<SearchShardIterator>> requests = groupByNode(shards);

            for (Map.Entry<Tuple<String, String>, List<SearchShardIterator>> entry : requests.entrySet()) {
                CanMatchRequest canMatchRequest = createCanMatchRequest(entry);
                List<CanMatchRequest.ShardLevelRequest> shardLevelRequests = canMatchRequest.getShardLevelRequests();

                if (entry.getKey().v2() == null) {
                    // no target node
                    for (CanMatchRequest.ShardLevelRequest shardLevelRequest : shardLevelRequests) {
                        results.consumeShardFailure(shardLevelRequest.getShardRequestIndex());
                        onOperation(shardLevelRequest.getShardRequestIndex(), null);
                    }
                    continue;
                }

                searchTransportService.sendCanMatch(getConnection(entry.getKey().v1(), entry.getKey().v2()), canMatchRequest,
                    task, new ActionListener<>() {
                        @Override
                        public void onResponse(CanMatchNodeResponse canMatchResponse) {
                            assert shardLevelRequests.size() == canMatchResponse.getResponses().size() &&
                                shardLevelRequests.size() == canMatchResponse.getFailures().size();
                            for (int i = 0; i < canMatchResponse.getResponses().size(); i++) {
                                SearchService.CanMatchResponse response = canMatchResponse.getResponses().get(i);
                                if (response != null) {
                                    response.setShardIndex(shardLevelRequests.get(i).getShardRequestIndex());
                                    results.consumeResult(response, () -> {
                                    });
                                    onOperation(response.getShardIndex(), response);
                                }
                            }
                            for (int i = 0; i < canMatchResponse.getFailures().size(); i++) {
                                Exception failure = canMatchResponse.getFailures().get(i);
                                if (failure != null) {
                                    results.consumeShardFailure(i);
                                    onOperationFailed(shardLevelRequests.get(i).getShardRequestIndex(), failure);
                                }
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            for (CanMatchRequest.ShardLevelRequest shardLevelRequest : shardLevelRequests) {
                                onOperationFailed(shardLevelRequest.getShardRequestIndex(), e);
                            }
                        }
                    }
                );
            }
        }

        private void onOperation(int idx, SearchService.CanMatchResponse response) {
            responses.set(idx, response);
            if (counter.incrementAndGet() == responses.length()) {
                finishPhase();
            }
        }

        private void onOperationFailed(int idx, Exception e) {
            responses.set(idx, e);
            if (counter.incrementAndGet() == shards.size()) {
                finishPhase();
            }
        }

        private void finishPhase() {
            List<SearchShardIterator> remainingShards = new ArrayList<>();
            for (SearchShardIterator ssi : shards) {
                int shardIndex = shardItIndexMap.get(ssi);
                Object resp = responses.get(shardIndex);
                if (resp instanceof Exception) {
                    // do something meaningful
                    remainingShards.add(ssi);
                }
            }
            if (remainingShards.isEmpty()) {
                finishHim();
            } else {
                // trigger another round
                new Round(new GroupShardsIterator<>(remainingShards)).start();
            }
        }
    }

    private CanMatchRequest createCanMatchRequest(Map.Entry<Tuple<String, String>, List<SearchShardIterator>> entry) {
        final SearchShardIterator first = entry.getValue().get(0);
        final List<CanMatchRequest.ShardLevelRequest> shardLevelRequests =
            entry.getValue().stream().map(this::buildShardLevelRequest)
                .collect(Collectors.toCollection(ArrayList::new));
        assert entry.getValue().stream().allMatch(ssi -> ssi.getOriginalIndices().equals(first.getOriginalIndices()));
        assert entry.getValue().stream().allMatch(ssi -> Objects.equals(ssi.getClusterAlias(), first.getClusterAlias()));
        final CanMatchRequest canMatchRequest = new CanMatchRequest(first.getOriginalIndices(), request,
            shardLevelRequests, getNumShards(), timeProvider.getAbsoluteStartMillis(), first.getClusterAlias());
        return canMatchRequest;
    }

    private void finishHim() {
        try {
            phaseFactory.apply(getIterator(results, shardsIts)).run();
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug(new ParameterizedMessage("Failed to execute [{}] while running [{}] phase", request, getName()), e);
            }
            onPhaseFailure(this, "", e);
        }
    }

    private static final float DEFAULT_INDEX_BOOST = 1.0f;

    public final CanMatchRequest.ShardLevelRequest buildShardLevelRequest(SearchShardIterator shardIt) {
        AliasFilter filter = aliasFilter.get(shardIt.shardId().getIndex().getUUID());
        assert filter != null;
        float indexBoost = concreteIndexBoosts.getOrDefault(shardIt.shardId().getIndex().getUUID(), DEFAULT_INDEX_BOOST);
        CanMatchRequest.ShardLevelRequest shardRequest = new CanMatchRequest.ShardLevelRequest(shardIt.shardId(),
            shardItIndexMap.get(shardIt), filter, indexBoost, shardIt.getSearchContextId(), shardIt.getSearchContextKeepAlive());
        return shardRequest;
    }

    private boolean checkMinimumVersion(GroupShardsIterator<SearchShardIterator> shardsIts) {
        for (SearchShardIterator it : shardsIts) {
            if (it.getTargetNodeIds().isEmpty() == false) {
                boolean isCompatible = it.getTargetNodeIds().stream().anyMatch(nodeId -> {
                    Transport.Connection conn = getConnection(it.getClusterAlias(), nodeId);
                    return conn == null ? true : conn.getVersion().onOrAfter(request.minCompatibleShardNode());
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
            //no search shards to search on, bail with empty response
            //(it happens with search across _all with no indices around and consistent with broadcast operations)
            int trackTotalHitsUpTo = request.source() == null ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO :
                request.source().trackTotalHitsUpTo() == null ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO :
                    request.source().trackTotalHitsUpTo();
            // total hits is null in the response if the tracking of total hits is disabled
            boolean withTotalHits = trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_DISABLED;
            listener.onResponse(new SearchResponse(InternalSearchResponse.empty(withTotalHits), null, 0, 0, 0, buildTookInMillis(),
                ShardSearchFailure.EMPTY_ARRAY, clusters, null));
            return;
        }

        try {
            run();
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug(new ParameterizedMessage("Failed to execute [{}] while running [{}] phase", request, getName()), e);
            }
            onPhaseFailure(this, "", e);
        }
    }


    public final void onPhaseFailure(SearchPhase phase, String msg, Throwable cause) {
        listener.onFailure(new SearchPhaseExecutionException(phase.getName(), msg, cause, ShardSearchFailure.EMPTY_ARRAY));
    }

    public final Transport.Connection getConnection(String clusterAlias, String nodeId) {
        Transport.Connection conn = nodeIdToConnection.apply(clusterAlias, nodeId);
        Version minVersion = request.minCompatibleShardNode();
        if (minVersion != null && conn != null && conn.getVersion().before(minVersion)) {
            throw new VersionMismatchException("One of the shards is incompatible with the required minimum version [{}]", minVersion);
        }
        return conn;
    }

    private int getNumShards() {
        return shardsIts.size();
    }

    long buildTookInMillis() {
        return timeProvider.buildTookInMillis();
    }


    private static final class CanMatchSearchPhaseResults extends SearchPhaseResults<SearchService.CanMatchResponse> {
        private final FixedBitSet possibleMatches;
        private final MinAndMax<?>[] minAndMaxes;
        private int numPossibleMatches;

        CanMatchSearchPhaseResults(int size) {
            super(size);
            possibleMatches = new FixedBitSet(size);
            minAndMaxes = new MinAndMax<?>[size];
        }

        @Override
        void consumeResult(SearchService.CanMatchResponse result, Runnable next) {
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
        Stream<SearchService.CanMatchResponse> getSuccessfulResults() {
            return Stream.empty();
        }
    }

    private GroupShardsIterator<SearchShardIterator> getIterator(CanMatchSearchPhaseResults results,
                                                                 GroupShardsIterator<SearchShardIterator> shardsIts) {
        int cardinality = results.getNumPossibleMatches();
        FixedBitSet possibleMatches = results.getPossibleMatches();
        if (cardinality == 0) {
            // this is a special case where we have no hit but we need to get at least one search response in order
            // to produce a valid search result with all the aggs etc.
            // Since it's possible that some of the shards that we're skipping are
            // unavailable, we would try to query the node that at least has some
            // shards available in order to produce a valid search result.
            int shardIndexToQuery = 0;
            for (int i = 0; i < shardsIts.size(); i++) {
                if (shardsIts.get(i).size() > 0) {
                    shardIndexToQuery = i;
                    break;
                }
            }
            possibleMatches.set(shardIndexToQuery);
        }
        SearchSourceBuilder source = request.source();
        int i = 0;
        for (SearchShardIterator iter : shardsIts) {
            if (possibleMatches.get(i++)) {
                iter.reset();
            } else {
                iter.resetAndSkip();
            }
        }
        if (shouldSortShards(results.minAndMaxes) == false) {
            return shardsIts;
        }
        FieldSortBuilder fieldSort = FieldSortBuilder.getPrimaryFieldSortOrNull(source);
        return new GroupShardsIterator<>(sortShards(shardsIts, results.minAndMaxes, fieldSort.order()));
    }

    private static List<SearchShardIterator> sortShards(GroupShardsIterator<SearchShardIterator> shardsIts,
                                                        MinAndMax<?>[] minAndMaxes,
                                                        SortOrder order) {
        return IntStream.range(0, shardsIts.size())
            .boxed()
            .sorted(shardComparator(shardsIts, minAndMaxes,  order))
            .map(shardsIts::get)
            .collect(Collectors.toList());
    }

    private static boolean shouldSortShards(MinAndMax<?>[] minAndMaxes) {
        Class<?> clazz = null;
        for (MinAndMax<?> minAndMax : minAndMaxes) {
            if (clazz == null) {
                clazz = minAndMax == null ? null : minAndMax.getMin().getClass();
            } else if (minAndMax != null && clazz != minAndMax.getMin().getClass()) {
                // we don't support sort values that mix different types (e.g.: long/double, numeric/keyword).
                // TODO: we could fail the request because there is a high probability
                //  that the merging of topdocs will fail later for the same reason ?
                return false;
            }
        }
        return clazz != null;
    }

    private static Comparator<Integer> shardComparator(GroupShardsIterator<SearchShardIterator> shardsIts,
                                                       MinAndMax<?>[] minAndMaxes,
                                                       SortOrder order) {
        final Comparator<Integer> comparator = Comparator.comparing(
            index -> minAndMaxes[index],
            forciblyCast(MinAndMax.getComparator(order))
        );

        return comparator.thenComparing(index -> shardsIts.get(index));
    }

}

