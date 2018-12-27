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
import org.elasticsearch.search.CCSInfo;
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

    public void testCCSInfo() {
        CCSInfo ccsInfo = randomBoolean() ? null : new CCSInfo(randomAlphaOfLengthBetween(5, 10), randomBoolean());
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomInt());
        SearchShardIterator searchShardIterator = new SearchShardIterator(ccsInfo, shardId, Collections.emptyList(), OriginalIndices.NONE);
        assertSame(ccsInfo, searchShardIterator.getCCSInfo());
        if (ccsInfo == null) {
            assertNull(searchShardIterator.getCCSInfo());
        } else {
            assertEquals(ccsInfo.getConnectionAlias(), searchShardIterator.getCCSInfo().getConnectionAlias());
        }
    }

    public void testNewSearchShardTarget() {
        CCSInfo ccsInfo = randomBoolean() ? null : new CCSInfo(randomAlphaOfLengthBetween(5, 10), randomBoolean());
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomInt());
        OriginalIndices originalIndices = new OriginalIndices(new String[]{randomAlphaOfLengthBetween(3, 10)},
            IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        SearchShardIterator searchShardIterator = new SearchShardIterator(ccsInfo, shardId, Collections.emptyList(), originalIndices);
        String nodeId = randomAlphaOfLengthBetween(3, 10);
        SearchShardTarget searchShardTarget = searchShardIterator.newSearchShardTarget(nodeId);
        if (ccsInfo == null) {
            assertNull(searchShardTarget.getConnectionAlias());
            assertNull(searchShardTarget.getHitIndexPrefix());
        } else {
            assertEquals(ccsInfo.getConnectionAlias(), searchShardTarget.getConnectionAlias());
            assertEquals(ccsInfo.getHitIndexPrefix(), searchShardTarget.getHitIndexPrefix());
        }
        assertSame(shardId, searchShardTarget.getShardId());
        assertEquals(nodeId, searchShardTarget.getNodeId());
        assertSame(originalIndices, searchShardTarget.getOriginalIndices());
    }
}
