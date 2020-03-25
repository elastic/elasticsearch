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

package org.elasticsearch.cluster.routing;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.OriginalIndicesTests;
import org.elasticsearch.action.search.SearchShardIterator;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;

public class GroupShardsIteratorTests extends ESTestCase {

    public static List<ShardRouting> randomShardRoutings(ShardId shardId) {
        return randomShardRoutings(shardId, randomIntBetween(0, 2));
    }

    private static List<ShardRouting> randomShardRoutings(ShardId shardId, int numReplicas) {
        List<ShardRouting> shardRoutings = new ArrayList<>();
        shardRoutings.add(TestShardRouting.newShardRouting(shardId, randomAlphaOfLengthBetween(5, 10), true, STARTED));
        for (int j = 0; j < numReplicas; j++) {
            shardRoutings.add(TestShardRouting.newShardRouting(shardId, randomAlphaOfLengthBetween(5, 10), false, STARTED));
        }
        return shardRoutings;
    }

    public void testSize() {
        List<ShardIterator> list = new ArrayList<>();
        Index index = new Index("foo", "na");
        {
            ShardId shardId = new ShardId(index, 0);
            list.add(new PlainShardIterator(shardId, randomShardRoutings(shardId, 2)));
        }
        list.add(new PlainShardIterator(new ShardId(index, 1), Collections.emptyList()));
        {
            ShardId shardId = new ShardId(index, 2);
            list.add(new PlainShardIterator(shardId, randomShardRoutings(shardId, 0)));
        }
        index = new Index("foo_1", "na");
        {
            ShardId shardId = new ShardId(index, 0);
            list.add(new PlainShardIterator(shardId, randomShardRoutings(shardId, 0)));
        }
        {
            ShardId shardId = new ShardId(index, 1);
            list.add(new PlainShardIterator(shardId, randomShardRoutings(shardId, 0)));
        }
        GroupShardsIterator<ShardIterator> iter = new GroupShardsIterator<>(list);
        assertEquals(7, iter.totalSizeWith1ForEmpty());
        assertEquals(5, iter.size());
        assertEquals(6, iter.totalSize());
    }

    public void testIterate() {
        List<ShardIterator> list = new ArrayList<>();
        Index index = new Index("foo", "na");
        {
            ShardId shardId = new ShardId(index, 0);
            list.add(new PlainShardIterator(shardId, randomShardRoutings(shardId)));
        }
        list.add(new PlainShardIterator(new ShardId(index, 1), Collections.emptyList()));
        {
            ShardId shardId = new ShardId(index, 2);
            list.add(new PlainShardIterator(shardId, randomShardRoutings(shardId)));
        }
        {
            ShardId shardId = new ShardId(index, 0);
            list.add(new PlainShardIterator(shardId, randomShardRoutings(shardId)));
        }
        {
            ShardId shardId = new ShardId(index, 1);
            list.add(new PlainShardIterator(shardId, randomShardRoutings(shardId)));
        }
        index = new Index("foo_2", "na");
        {
            ShardId shardId = new ShardId(index, 0);
            list.add(new PlainShardIterator(shardId, randomShardRoutings(shardId)));
        }
        {
            ShardId shardId = new ShardId(index, 1);
            list.add(new PlainShardIterator(shardId, randomShardRoutings(shardId)));
        }

        Collections.shuffle(list, random());
        {
            GroupShardsIterator<ShardIterator> unsorted = new GroupShardsIterator<>(list);
            GroupShardsIterator<ShardIterator> iter = new GroupShardsIterator<>(list);
            List<ShardIterator> actualIterators = new ArrayList<>();
            for (ShardIterator shardsIterator : iter) {
                actualIterators.add(shardsIterator);
            }
            assertEquals(actualIterators, list);
        }
        {
            GroupShardsIterator<ShardIterator> iter = GroupShardsIterator.sortAndCreate(list);
            List<ShardIterator> actualIterators = new ArrayList<>();
            for (ShardIterator shardsIterator : iter) {
                actualIterators.add(shardsIterator);
            }
            CollectionUtil.timSort(actualIterators);
            assertEquals(actualIterators, list);
        }
    }

    public void testOrderingWithSearchShardIterators() {
        String[] indices = generateRandomStringArray(10, 10, false, false);
        Arrays.sort(indices);
        String[] uuids = generateRandomStringArray(5, 10, false, false);
        Arrays.sort(uuids);
        String[] clusters = generateRandomStringArray(5, 10, false, false);
        Arrays.sort(clusters);

        List<SearchShardIterator> sorted = new ArrayList<>();
        int numShards = randomIntBetween(1, 10);
        for (int i = 0; i < numShards; i++) {
            for (String index : indices) {
                for (String uuid : uuids) {
                    ShardId shardId = new ShardId(index, uuid, i);
                    SearchShardIterator shardIterator = new SearchShardIterator(null, shardId,
                        GroupShardsIteratorTests.randomShardRoutings(shardId), OriginalIndicesTests.randomOriginalIndices());
                    sorted.add(shardIterator);
                    for (String cluster : clusters) {
                        SearchShardIterator remoteIterator = new SearchShardIterator(cluster, shardId,
                            GroupShardsIteratorTests.randomShardRoutings(shardId), OriginalIndicesTests.randomOriginalIndices());
                        sorted.add(remoteIterator);
                    }
                }
            }
        }

        List<SearchShardIterator> shuffled = new ArrayList<>(sorted);
        Collections.shuffle(shuffled, random());
        {
            List<ShardIterator> actualIterators = new ArrayList<>();
            GroupShardsIterator<SearchShardIterator> iter = new GroupShardsIterator<>(shuffled);
            for (SearchShardIterator searchShardIterator : iter) {
                actualIterators.add(searchShardIterator);
            }
            assertEquals(shuffled, actualIterators);
        }
        {
            List<ShardIterator> actualIterators = new ArrayList<>();
            GroupShardsIterator<SearchShardIterator> iter = GroupShardsIterator.sortAndCreate(shuffled);
            for (SearchShardIterator searchShardIterator : iter) {
                actualIterators.add(searchShardIterator);
            }
            assertEquals(sorted, actualIterators);
        }
    }
}
