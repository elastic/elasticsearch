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
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

public class SearchShardIteratorTests extends ESTestCase {

    public void testShardId() {
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomInt());
        SearchShardIterator searchShardIterator = new SearchShardIterator(null, shardId, Collections.emptyList(), OriginalIndices.NONE);
        assertSame(shardId, searchShardIterator.shardId());
    }

    public void testGetOriginalIndices() {
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomInt());
        OriginalIndices originalIndices = new OriginalIndices(new String[]{randomAlphaOfLengthBetween(3, 10)},
            IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        SearchShardIterator searchShardIterator = new SearchShardIterator(null, shardId, Collections.emptyList(), originalIndices);
        assertSame(originalIndices, searchShardIterator.getOriginalIndices());
    }

    public void testGetClusterAlias() {
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomInt());
        SearchShardIterator searchShardIterator = new SearchShardIterator(clusterAlias, shardId, Collections.emptyList(),
            OriginalIndices.NONE);
        assertEquals(clusterAlias, searchShardIterator.getClusterAlias());
    }

    public void testNewSearchShardTarget() {
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomInt());
        OriginalIndices originalIndices = new OriginalIndices(new String[]{randomAlphaOfLengthBetween(3, 10)},
            IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        SearchShardIterator searchShardIterator = new SearchShardIterator(clusterAlias, shardId, Collections.emptyList(), originalIndices);
        String nodeId = randomAlphaOfLengthBetween(3, 10);
        SearchShardTarget searchShardTarget = searchShardIterator.newSearchShardTarget(nodeId);
        assertEquals(clusterAlias, searchShardTarget.getClusterAlias());
        assertSame(shardId, searchShardTarget.getShardId());
        assertEquals(nodeId, searchShardTarget.getNodeId());
        assertSame(originalIndices, searchShardTarget.getOriginalIndices());
    }
}
