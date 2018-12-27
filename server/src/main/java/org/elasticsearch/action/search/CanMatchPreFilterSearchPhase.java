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
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.transport.Transport;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * This search phase can be used as an initial search phase to pre-filter search shards based on query rewriting.
 * The queries are rewritten against the shards and based on the rewrite result shards might be able to be excluded
 * from the search. The extra round trip to the search shards is very cheap and is not subject to rejections
 * which allows to fan out to more shards at the same time without running into rejections even if we are hitting a
 * large portion of the clusters indices.
 */
final class CanMatchPreFilterSearchPhase extends AbstractSearchAsyncAction<SearchService.CanMatchResponse> {

    private final Function<GroupShardsIterator<SearchShardIterator>, SearchPhase> phaseFactory;
    private final GroupShardsIterator<SearchShardIterator> shardsIts;

    CanMatchPreFilterSearchPhase(Logger logger, SearchTransportService searchTransportService,
                                        BiFunction<String, String, Transport.Connection> nodeIdToConnection,
                                        Map<String, AliasFilter> aliasFilter, Map<String, Float> concreteIndexBoosts,
                                        Map<String, Set<String>> indexRoutings,
                                        Executor executor, SearchRequest request,
                                        ActionListener<SearchResponse> listener, GroupShardsIterator<SearchShardIterator> shardsIts,
                                        TransportSearchAction.SearchTimeProvider timeProvider, long clusterStateVersion,
                                        SearchTask task, Function<GroupShardsIterator<SearchShardIterator>, SearchPhase> phaseFactory,
                                        SearchResponse.Clusters clusters) {
        //We set max concurrent shard requests to the number of shards so no throttling happens for can_match requests
        super("can_match", logger, searchTransportService, nodeIdToConnection, aliasFilter, concreteIndexBoosts, indexRoutings,
                executor, request, listener, shardsIts, timeProvider, clusterStateVersion, task,
                new BitSetSearchPhaseResults(shardsIts.size()), shardsIts.size(), clusters);
        this.phaseFactory = phaseFactory;
        this.shardsIts = shardsIts;
    }

    @Override
    protected void executePhaseOnShard(SearchShardIterator shardIt, ShardRouting shard,
                                       SearchActionListener<SearchService.CanMatchResponse> listener) {
        getSearchTransport().sendCanMatch(getConnection(shardIt.getClusterAlias(), shard.currentNodeId()),
            buildShardSearchRequest(shardIt), getTask(), listener);
    }

    @Override
    protected SearchPhase getNextPhase(SearchPhaseResults<SearchService.CanMatchResponse> results,
                                       SearchPhaseContext context) {

        return phaseFactory.apply(getIterator((BitSetSearchPhaseResults) results, shardsIts));
    }

    private GroupShardsIterator<SearchShardIterator> getIterator(BitSetSearchPhaseResults results,
                                                                 GroupShardsIterator<SearchShardIterator> shardsIts) {
        int cardinality = results.getNumPossibleMatches();
        FixedBitSet possibleMatches = results.getPossibleMatches();
        if (cardinality == 0) {
            // this is a special case where we have no hit but we need to get at least one search response in order
            // to produce a valid search result with all the aggs etc.
            possibleMatches.set(0);
        }
        int i = 0;
        for (SearchShardIterator iter : shardsIts) {
            if (possibleMatches.get(i++)) {
                iter.reset();
            } else {
                iter.resetAndSkip();
            }
        }
        return shardsIts;
    }

    private static final class BitSetSearchPhaseResults extends InitialSearchPhase.
        SearchPhaseResults<SearchService.CanMatchResponse> {

        private final FixedBitSet possibleMatches;
        private int numPossibleMatches;

        BitSetSearchPhaseResults(int size) {
            super(size);
            possibleMatches = new FixedBitSet(size);
        }

        @Override
        void consumeResult(SearchService.CanMatchResponse result) {
            if (result.canMatch()) {
                consumeShardFailure(result.getShardIndex());
            }
        }

        @Override
        boolean hasResult(int shardIndex) {
            return false; // unneeded
        }

        @Override
        synchronized void consumeShardFailure(int shardIndex) {
            // we have to carry over shard failures in order to account for them in the response.
            possibleMatches.set(shardIndex);
            numPossibleMatches++;
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
}
