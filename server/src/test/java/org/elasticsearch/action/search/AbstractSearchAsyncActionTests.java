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

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class AbstractSearchAsyncActionTests extends ESTestCase {

    private AbstractSearchAsyncAction<SearchPhaseResult> createAction(
            final GroupShardsIterator<SearchShardIterator> shardsIt,
            final boolean controlled,
            final AtomicLong expected) {

        final Runnable runnable;
        final TransportSearchAction.SearchTimeProvider timeProvider;
        if (controlled) {
            runnable = () -> expected.set(randomNonNegativeLong());
            timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, expected::get);
        } else {
            runnable = () -> {
                long elapsed = spinForAtLeastNMilliseconds(randomIntBetween(1, 10));
                expected.set(elapsed);
            };
            timeProvider = new TransportSearchAction.SearchTimeProvider(
                    0,
                    System.nanoTime(),
                    System::nanoTime);
        }

        final SearchRequest request = new SearchRequest();
        Map<String, AliasFilter> aliasFilterMap = new HashMap<>();
        Map<String, Float> concreteIndexBoosts = new HashMap<>();
        for (SearchShardIterator it : shardsIt) {
            aliasFilterMap.put(it.shardId().getIndex().getUUID(), new AliasFilter(new MatchAllQueryBuilder()));
            concreteIndexBoosts.put(it.shardId().getIndex().getUUID(), 2.0f);
        }
        request.allowPartialSearchResults(true);
        return new AbstractSearchAsyncAction<SearchPhaseResult>("test", null, null, null,
                aliasFilterMap, concreteIndexBoosts, null,
                request, null, shardsIt, timeProvider, 0, null,
                new InitialSearchPhase.ArraySearchPhaseResults<>(shardsIt.size()), request.getMaxConcurrentShardRequests(),
                SearchResponse.Clusters.EMPTY) {
            @Override
            protected SearchPhase getNextPhase(final SearchPhaseResults<SearchPhaseResult> results, final SearchPhaseContext context) {
                return null;
            }

            @Override
            protected void executePhaseOnShard(final SearchShardIterator shardIt, final ShardRouting shard,
                                               final SearchActionListener<SearchPhaseResult> listener) {
            }

            @Override
            long buildTookInMillis() {
                runnable.run();
                return super.buildTookInMillis();
            }
        };
    }

    public void testTookWithControlledClock() {
        runTestTook(true);
    }

    public void testTookWithRealClock() {
        runTestTook(false);
    }

    private void runTestTook(final boolean controlled) {
        final AtomicLong expected = new AtomicLong();
        GroupShardsIterator<SearchShardIterator> shardsIt = new GroupShardsIterator<>(
            Collections.singletonList(
                new SearchShardIterator(null, new ShardId(new Index("name", "foo"), 0),
                    Collections.emptyList(), null)
            )
        );
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(shardsIt, controlled, expected);
        final long actual = action.buildTookInMillis();
        if (controlled) {
            // with a controlled clock, we can assert the exact took time
            assertThat(actual, equalTo(TimeUnit.NANOSECONDS.toMillis(expected.get())));
        } else {
            // with a real clock, the best we can say is that it took as long as we spun for
            assertThat(actual, greaterThanOrEqualTo(TimeUnit.NANOSECONDS.toMillis(expected.get())));
        }
    }

    public void testBuildShardSearchTransportRequest() {
        final AtomicLong expected = new AtomicLong();
        GroupShardsIterator<SearchShardIterator> shardsIt = new GroupShardsIterator<>(
            Collections.singletonList(
                new SearchShardIterator(null, new ShardId(new Index("name", "foo"), 1),
                    Collections.emptyList(), null)
            )
        );
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(shardsIt, false, expected);
        SearchShardIterator iterator = new SearchShardIterator("test-cluster", new ShardId(new Index("name", "foo"), 1),
            Collections.emptyList(), new OriginalIndices(new String[] {"name", "name1"}, IndicesOptions.strictExpand()));
        ShardSearchTransportRequest shardSearchTransportRequest = action.buildShardSearchRequest(iterator);
        assertEquals(IndicesOptions.strictExpand(), shardSearchTransportRequest.indicesOptions());
        assertArrayEquals(new String[] {"name", "name1"}, shardSearchTransportRequest.indices());
        assertEquals(new MatchAllQueryBuilder(), shardSearchTransportRequest.getAliasFilter().getQueryBuilder());
        assertEquals(2.0f, shardSearchTransportRequest.indexBoost(), 0.0f);
        assertEquals(0, shardSearchTransportRequest.remapShardId());
        assertEquals(1, shardSearchTransportRequest.numberOfIndexShards());
        assertEquals(1, shardSearchTransportRequest.numberOfShards());
    }

    public void testBuildFilteredShardSearchTransportRequest() {
        final AtomicLong expected = new AtomicLong();
        int numIndex = randomIntBetween(1, 5);
        List<SearchShardIterator> shards = new ArrayList<>();
        int[] numIndexShards = new int[numIndex];
        Map<Integer, Map<Integer, Integer>> remapShards = new HashMap<>();
        int totalShards = 0;
        for (int i = 0; i < numIndex; i++) {
            int numShards = randomIntBetween(1, 10);
            numIndexShards[i] = numShards;
            String indexName = Integer.toString(i);
            int shardIndex = 0;
            Map<Integer, Integer> shardMap = new HashMap<>();
            for (int j = 0; j < numShards; j++) {
                if (randomBoolean()) {
                    shards.add(new SearchShardIterator(null, new ShardId(new Index(indexName, indexName), j),
                        Collections.emptyList(), null));
                    shardMap.put(j, shardIndex++);
                    totalShards++;
                }
            }
            remapShards.put(i, shardMap);
        }
        Collections.shuffle(shards, random());
        GroupShardsIterator<SearchShardIterator> shardsIt = new GroupShardsIterator<>(shards);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(shardsIt,false, expected);

        for (int i = 0; i < numIndex; i++) {
            int numShards = numIndexShards[i];
            String indexName = Integer.toString(i);
            Map<Integer, Integer> shardMap = remapShards.get(i);
            if (shardMap.size() == 0) {
                continue;
            }
            for (int j = 0; j < numShards; j++) {
                if (shardMap.containsKey(j) == false) {
                    continue;
                }
                SearchShardIterator iterator = new SearchShardIterator("test-cluster", new ShardId(new Index(indexName, indexName), j),
                    Collections.emptyList(), new OriginalIndices(new String[]{"name", "name1"}, IndicesOptions.strictExpand()));
                ShardSearchTransportRequest shardSearchTransportRequest = action.buildShardSearchRequest(iterator);
                assertThat(shardSearchTransportRequest.numberOfIndexShards(), equalTo(shardMap.size()));
                assertThat(shardSearchTransportRequest.remapShardId(), equalTo(shardMap.get(j)));
                assertThat(shardSearchTransportRequest.numberOfShards(), equalTo(totalShards));
            }
        }
    }
}
