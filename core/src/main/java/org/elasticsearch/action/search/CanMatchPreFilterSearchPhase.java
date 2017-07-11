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
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.transport.Transport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * This search phrase can be used as an initial search phase to pre-filter search shards based on query rewriting.
 * The queries are rewritten against the shards and based on the rewrite result shards might be able to be excluded
 * from the search. The extra round trip to the search shards is very cheap and is not subject to rejections
 * which allows to fan out to more shards at the same time without running into rejections even if we are hitting a
 * large portion of the clusters indices.
 */
final class CanMatchPreFilterSearchPhase extends AbstractSearchAsyncAction<SearchTransportService.CanMatchResponse> {

    private final Function<GroupShardsIterator<SearchShardIterator>, SearchPhase> phaseFactory;
    private final GroupShardsIterator<SearchShardIterator> shardsIts;

    CanMatchPreFilterSearchPhase(Logger logger, SearchTransportService searchTransportService,
                                        BiFunction<String, String, Transport.Connection> nodeIdToConnection,
                                        Map<String, AliasFilter> aliasFilter, Map<String, Float> concreteIndexBoosts,
                                        Executor executor, SearchRequest request,
                                        ActionListener<SearchResponse> listener, GroupShardsIterator<SearchShardIterator> shardsIts,
                                        TransportSearchAction.SearchTimeProvider timeProvider, long clusterStateVersion,
                                        SearchTask task, Function<GroupShardsIterator<SearchShardIterator>, SearchPhase> phaseFactory) {
        super("can_match", logger, searchTransportService, nodeIdToConnection, aliasFilter, concreteIndexBoosts, executor, request,
            listener,
            shardsIts, timeProvider, clusterStateVersion, task, new BitSetSearchPhaseResults(shardsIts.size()));
        this.phaseFactory = phaseFactory;
        this.shardsIts = shardsIts;
    }

    @Override
    protected void executePhaseOnShard(SearchShardIterator shardIt, ShardRouting shard,
                                       SearchActionListener<SearchTransportService.CanMatchResponse> listener) {
        getSearchTransport().sendCanMatch(getConnection(shardIt.getClusterAlias(), shard.currentNodeId()),
            buildShardSearchRequest(shardIt), getTask(), listener);
    }

    @Override
    protected SearchPhase getNextPhase(SearchPhaseResults<SearchTransportService.CanMatchResponse> results,
                                       SearchPhaseContext context) {
        return phaseFactory.apply(getIterator((BitSetSearchPhaseResults) results, shardsIts));
    }

    private GroupShardsIterator<SearchShardIterator> getIterator(BitSetSearchPhaseResults results,
                                                                 GroupShardsIterator<SearchShardIterator> shardsIts) {
        if (results.numMatches == shardsIts.size()) {
            shardsIts.iterator().forEachRemaining(i -> i.reset());
            return shardsIts;
        } else if (results.numMatches == 0) {
            // this is a special case where we have no hit but we need to get at least one search response in order
            // to produce a valid search result with all the aggs etc. at least that is what I think is the case... and clint does so
            // too :D
            Iterator<SearchShardIterator> iterator = shardsIts.iterator();
            if (iterator.hasNext()) {
                SearchShardIterator next = iterator.next();
                next.reset();
                return new GroupShardsIterator<>(Collections.singletonList(next));
            }
        }
        List<SearchShardIterator> shardIterators = new ArrayList<>(results.numMatches);
        int i = 0;
        for (SearchShardIterator iter : shardsIts) {
            if (results.possibleMatches.get(i++)) {
                iter.reset();
                shardIterators.add(iter);
            }
        }
        return new GroupShardsIterator<>(shardIterators);
    }

    private static final class BitSetSearchPhaseResults extends InitialSearchPhase.
        SearchPhaseResults<SearchTransportService.CanMatchResponse> {

        private FixedBitSet possibleMatches;
        private int numMatches;

        BitSetSearchPhaseResults(int size) {
            super(size);
            possibleMatches = new FixedBitSet(size);
        }

        @Override
        void consumeResult(SearchTransportService.CanMatchResponse result) {
            if (result.canMatch()) {
                synchronized (possibleMatches) {
                    possibleMatches.set(result.getShardIndex());
                    numMatches++;
                }
            }
        }

        @Override
        boolean hasResult(int shardIndex) {
            synchronized (possibleMatches) {
                return possibleMatches.get(shardIndex);
            }
        }

        @Override
        Stream<SearchTransportService.CanMatchResponse> getSuccessfulResults() {
            return Stream.empty();
        }
    }
}
