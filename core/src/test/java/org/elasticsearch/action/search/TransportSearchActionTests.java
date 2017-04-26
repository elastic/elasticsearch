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
import org.elasticsearch.cluster.routing.PlainShardIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;

public class TransportSearchActionTests extends ESTestCase {

    public void testMergeShardsIterators() throws IOException {
        List<ShardIterator> localShardIterators = new ArrayList<>();
        {
            ShardId shardId = new ShardId("local_index", "local_index_uuid", 0);
            ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, "local_node", true, STARTED);
            ShardIterator shardIterator = new PlainShardIterator(shardId, Collections.singletonList(shardRouting));
            localShardIterators.add(shardIterator);
        }
        {
            ShardId shardId2 = new ShardId("local_index_2", "local_index_2_uuid", 1);
            ShardRouting shardRouting2 = TestShardRouting.newShardRouting(shardId2, "local_node", true, STARTED);
            ShardIterator shardIterator2 = new PlainShardIterator(shardId2, Collections.singletonList(shardRouting2));
            localShardIterators.add(shardIterator2);
        }
        GroupShardsIterator<ShardIterator> localShardsIterator = new GroupShardsIterator<>(localShardIterators);

        OriginalIndices localIndices = new OriginalIndices(new String[]{"local_alias", "local_index_2"},
                IndicesOptions.strictExpandOpenAndForbidClosed());

        OriginalIndices remoteIndices = new OriginalIndices(new String[]{"remote_alias", "remote_index_2"},
                IndicesOptions.strictExpandOpen());
        List<SearchShardIterator> remoteShardIterators = new ArrayList<>();
        {
            ShardId remoteShardId = new ShardId("remote_index", "remote_index_uuid", 2);
            ShardRouting remoteShardRouting = TestShardRouting.newShardRouting(remoteShardId, "remote_node", true, STARTED);
            SearchShardIterator remoteShardIterator = new SearchShardIterator(remoteShardId,
                    Collections.singletonList(remoteShardRouting), remoteIndices);
            remoteShardIterators.add(remoteShardIterator);
        }
        {
            ShardId remoteShardId2 = new ShardId("remote_index_2", "remote_index_2_uuid", 3);
            ShardRouting remoteShardRouting2 = TestShardRouting.newShardRouting(remoteShardId2, "remote_node", true, STARTED);
            SearchShardIterator remoteShardIterator2 = new SearchShardIterator(remoteShardId2,
                    Collections.singletonList(remoteShardRouting2), remoteIndices);
            remoteShardIterators.add(remoteShardIterator2);
        }
        OriginalIndices remoteIndices2 = new OriginalIndices(new String[]{"remote_index_3"}, IndicesOptions.strictExpand());

        {
            ShardId remoteShardId3 = new ShardId("remote_index_3", "remote_index_3_uuid", 4);
            ShardRouting remoteShardRouting3 = TestShardRouting.newShardRouting(remoteShardId3, "remote_node", true, STARTED);
            SearchShardIterator remoteShardIterator3 = new SearchShardIterator(remoteShardId3,
                    Collections.singletonList(remoteShardRouting3), remoteIndices2);
            remoteShardIterators.add(remoteShardIterator3);
        }

        GroupShardsIterator<SearchShardIterator> searchShardIterators = TransportSearchAction.mergeShardsIterators(localShardsIterator,
                localIndices, remoteShardIterators);

        assertEquals(searchShardIterators.size(), 5);
        int i = 0;
        for (SearchShardIterator searchShardIterator : searchShardIterators) {
            switch(i++) {
                case 0:
                    assertEquals("local_index", searchShardIterator.shardId().getIndexName());
                    assertEquals(0, searchShardIterator.shardId().getId());
                    assertSame(localIndices, searchShardIterator.getOriginalIndices());
                    break;
                case 1:
                    assertEquals("local_index_2", searchShardIterator.shardId().getIndexName());
                    assertEquals(1, searchShardIterator.shardId().getId());
                    assertSame(localIndices, searchShardIterator.getOriginalIndices());
                    break;
                case 2:
                    assertEquals("remote_index", searchShardIterator.shardId().getIndexName());
                    assertEquals(2, searchShardIterator.shardId().getId());
                    assertSame(remoteIndices, searchShardIterator.getOriginalIndices());
                    break;
                case 3:
                    assertEquals("remote_index_2", searchShardIterator.shardId().getIndexName());
                    assertEquals(3, searchShardIterator.shardId().getId());
                    assertSame(remoteIndices, searchShardIterator.getOriginalIndices());
                    break;
                case 4:
                    assertEquals("remote_index_3", searchShardIterator.shardId().getIndexName());
                    assertEquals(4, searchShardIterator.shardId().getId());
                    assertSame(remoteIndices2, searchShardIterator.getOriginalIndices());
                    break;
            }
        }
    }
}
