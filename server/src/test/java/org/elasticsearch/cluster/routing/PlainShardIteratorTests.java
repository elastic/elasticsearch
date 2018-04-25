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

import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;

public class PlainShardIteratorTests extends ESTestCase {

    public void testEquals() {
        Index index = new Index("a", "b");
        ShardId shardId = new ShardId(index, 1);
        ShardId shardId2 = new ShardId(index, 2);
        PlainShardIterator iterator1 = new PlainShardIterator(shardId, new ArrayList<>());
        PlainShardIterator iterator2 = new PlainShardIterator(shardId, new ArrayList<>());
        PlainShardIterator iterator3 = new PlainShardIterator(shardId2, new ArrayList<>());
        String s = "Some other random object";
        assertEquals(iterator1, iterator1);
        assertEquals(iterator1, iterator2);
        assertNotEquals(iterator1, null);
        assertNotEquals(iterator1, s);
        assertNotEquals(iterator1, iterator3);
    }
}
