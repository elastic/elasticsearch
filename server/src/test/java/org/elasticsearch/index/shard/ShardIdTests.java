/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

public class ShardIdTests extends ESTestCase {

    public void testShardIdFromString() {
        String indexName = randomAlphaOfLengthBetween(3,50);
        int shardId = randomInt();
        ShardId id = ShardId.fromString("["+indexName+"]["+shardId+"]");
        assertEquals(indexName, id.getIndexName());
        assertEquals(shardId, id.getId());
        assertEquals(indexName, id.getIndex().getName());
        assertEquals(IndexMetadata.INDEX_UUID_NA_VALUE, id.getIndex().getUUID());

        id = ShardId.fromString("[some]weird[0]Name][-125]");
        assertEquals("some]weird[0]Name", id.getIndexName());
        assertEquals(-125, id.getId());
        assertEquals("some]weird[0]Name", id.getIndex().getName());
        assertEquals(IndexMetadata.INDEX_UUID_NA_VALUE, id.getIndex().getUUID());

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

    public void testEquals() {
        Index index1 = new Index("a", "a");
        Index index2 = new Index("a", "b");
        ShardId shardId1 = new ShardId(index1, 0);
        ShardId shardId2 = new ShardId(index1, 0);
        ShardId shardId3 = new ShardId(index2, 0);
        ShardId shardId4 = new ShardId(index1, 1);
        String s = "Some random other object";
        assertEquals(shardId1, shardId1);
        assertEquals(shardId1, shardId2);
        assertNotEquals(shardId1, null);
        assertNotEquals(shardId1, s);
        assertNotEquals(shardId1, shardId3);
        assertNotEquals(shardId1, shardId4);
    }
}
