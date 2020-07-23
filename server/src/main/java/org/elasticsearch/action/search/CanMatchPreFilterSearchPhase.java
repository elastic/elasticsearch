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
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.search.SearchService.CanMatchResponse;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.MinAndMax;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.Transport;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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
final class CanMatchPreFilterSearchPhase extends AbstractSearchAsyncAction<CanMatchResponse> {

    private final Function<GroupShardsIterator<SearchShardIterator>, SearchPhase> phaseFactory;
    private final GroupShardsIterator<SearchShardIterator> shardsIts;

    CanMatchPreFilterSearchPhase(Logger logger, SearchTransportService searchTransportService,
                                 BiFunction<String, String, Transport.Connection> nodeIdToConnection,
                                 Map<String, AliasFilter> aliasFilter, Map<String, Float> concreteIndexBoosts,
                                 Map<String, Set<String>> indexRoutings,
                                 Executor executor, SearchRequest request,
                                 ActionListener<SearchResponse> listener, GroupShardsIterator<SearchShardIterator> shardsIts,
                                 TransportSearchAction.SearchTimeProvider timeProvider, ClusterState clusterState,
                                 SearchTask task, Function<GroupShardsIterator<SearchShardIterator>, SearchPhase> phaseFactory,
                                 SearchResponse.Clusters clusters) {
        //We set max concurrent shard requests to the number of shards so no throttling happens for can_match requests
        super("can_match", logger, searchTransportService, nodeIdToConnection, aliasFilter, concreteIndexBoosts, indexRoutings,
                executor, request, listener, shardsIts, timeProvider, clusterState, task,
                new CanMatchSearchPhaseResults(shardsIts.size()), shardsIts.size(), clusters);
        this.phaseFactory = phaseFactory;
        this.shardsIts = shardsIts;
    }

    @Override
    protected void executePhaseOnShard(SearchShardIterator shardIt, ShardRouting shard,
                                       SearchActionListener<CanMatchResponse> listener) {
        getSearchTransport().sendCanMatch(getConnection(shardIt.getClusterAlias(), shard.currentNodeId()),
            buildShardSearchRequest(shardIt), getTask(), listener);
    }

    @Override
    protected SearchPhase getNextPhase(SearchPhaseResults<CanMatchResponse> results,
                                       SearchPhaseContext context) {

        return phaseFactory.apply(getIterator((CanMatchSearchPhaseResults) results, shardsIts));
    }

    private GroupShardsIterator<SearchShardIterator> getIterator(CanMatchSearchPhaseResults results,
                                                                 GroupShardsIterator<SearchShardIterator> shardsIts) {
        int cardinality = results.getNumPossibleMatches();
        FixedBitSet possibleMatches = results.getPossibleMatches();
        if (cardinality == 0) {
            // this is a special case where we have no hit but we need to get at least one search response in order
            // to produce a valid search result with all the aggs etc.
            possibleMatches.set(0);
        }
        SearchSourceBuilder source = getRequest().source();
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
        final Comparator<Integer> comparator = Comparator.comparing(index -> minAndMaxes[index], MinAndMax.getComparator(order));
        return comparator.thenComparing(index -> shardsIts.get(index).shardId());
    }

    private static final class CanMatchSearchPhaseResults extends SearchPhaseResults<CanMatchResponse> {
        private final FixedBitSet possibleMatches;
        private final MinAndMax<?>[] minAndMaxes;
        private int numPossibleMatches;

        CanMatchSearchPhaseResults(int size) {
            super(size);
            possibleMatches = new FixedBitSet(size);
            minAndMaxes = new MinAndMax[size];
        }

        @Override
        void consumeResult(CanMatchResponse result) {
            consumeResult(result.getShardIndex(), result.canMatch(), result.estimatedMinAndMax());
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
        Stream<CanMatchResponse> getSuccessfulResults() {
            return Stream.empty();
        }
    }
}
