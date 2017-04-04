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

package org.elasticsearch.index.shard;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.test.ESTestCase;

public class ShardIdTests extends ESTestCase {

    public void testShardIdFromString() {
        String indexName = randomAlphaOfLengthBetween(3,50);
        int shardId = randomInt();
        ShardId id = ShardId.fromString("["+indexName+"]["+shardId+"]");
        assertEquals(indexName, id.getIndexName());
        assertEquals(shardId, id.getId());
        assertEquals(indexName, id.getIndex().getName());
        assertEquals(IndexMetaData.INDEX_UUID_NA_VALUE, id.getIndex().getUUID());

        id = ShardId.fromString("[some]weird[0]Name][-125]");
        assertEquals("some]weird[0]Name", id.getIndexName());
        assertEquals(-125, id.getId());
        assertEquals("some]weird[0]Name", id.getIndex().getName());
        assertEquals(IndexMetaData.INDEX_UUID_NA_VALUE, id.getIndex().getUUID());

        String badId = indexName + "," + shardId; // missing separator
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> ShardId.fromString(badId));
        assertEquals("Unexpected shardId string format, expected [indexName][shardId] but got " + badId, ex.getMessage());

        String badId2 = indexName + "][" + shardId + "]"; // missing opening bracket
        ex = expectThrows(IllegalArgumentException.class,
                () -> ShardId.fromString(badId2));

        String badId3 = "[" + indexName + "][" + shardId; // missing closing bracket
        ex = expectThrows(IllegalArgumentException.class,
                () -> ShardId.fromString(badId3));
    }
}
