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
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GroupShardsIteratorTests extends ESTestCase {

    public void testSize() {
        List<ShardIterator> list = new ArrayList<>();
        Index index = new Index("foo", "na");

        list.add(new PlainShardIterator(new ShardId(index, 0), Arrays.asList(newRouting(index, 0, true), newRouting(index, 0, true),
            newRouting(index, 0, true))));
        list.add(new PlainShardIterator(new ShardId(index, 1), Collections.emptyList()));
        list.add(new PlainShardIterator(new ShardId(index, 2), Arrays.asList(newRouting(index, 2, true))));
        index = new Index("foo_1", "na");

        list.add(new PlainShardIterator(new ShardId(index, 0), Arrays.asList(newRouting(index, 0, true))));
        list.add(new PlainShardIterator(new ShardId(index, 1), Arrays.asList(newRouting(index, 1, true))));
        GroupShardsIterator iter = new GroupShardsIterator<>(list);
        assertEquals(7, iter.totalSizeWith1ForEmpty());
        assertEquals(5, iter.size());
        assertEquals(6, iter.totalSize());
    }

    public void testIterate() {
        List<ShardIterator> list = new ArrayList<>();
        Index index = new Index("foo", "na");

        list.add(new PlainShardIterator(new ShardId(index, 0), Arrays.asList(newRouting(index, 0, true), newRouting(index, 0, true),
            newRouting(index, 0, true))));
        list.add(new PlainShardIterator(new ShardId(index, 1), Collections.emptyList()));
        list.add(new PlainShardIterator(new ShardId(index, 2), Arrays.asList(newRouting(index, 2, true))));

        list.add(new PlainShardIterator(new ShardId(index, 0), Arrays.asList(newRouting(index, 0, true))));
        list.add(new PlainShardIterator(new ShardId(index, 1), Arrays.asList(newRouting(index, 1, true))));

        index = new Index("foo_2", "na");
        list.add(new PlainShardIterator(new ShardId(index, 0), Arrays.asList(newRouting(index, 0, true))));
        list.add(new PlainShardIterator(new ShardId(index, 1), Arrays.asList(newRouting(index, 1, true))));

        Collections.shuffle(list, random());
        ArrayList<ShardIterator> actualIterators = new ArrayList<>();
        GroupShardsIterator<ShardIterator> iter = new GroupShardsIterator<>(list);
        for (ShardIterator shardsIterator : iter) {
            actualIterators.add(shardsIterator);
        }
        CollectionUtil.timSort(actualIterators);
        assertEquals(actualIterators, list);
    }

    public ShardRouting newRouting(Index index, int id, boolean started) {
        ShardRouting shardRouting = ShardRouting.newUnassigned(new ShardId(index, id), true,
            RecoverySource.StoreRecoverySource.EMPTY_STORE_INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo"));
        shardRouting = ShardRoutingHelper.initialize(shardRouting, "some node");
        if (started) {
            shardRouting = ShardRoutingHelper.moveToStarted(shardRouting);
        }
        return shardRouting;
    };
}
