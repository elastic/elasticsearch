/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ShardIdTests extends AbstractWireSerializingTestCase<ShardId> {

    @Override
    protected Writeable.Reader<ShardId> instanceReader() {
        return ShardId::new;
    }

    @Override
    protected ShardId createTestInstance() {
        return new ShardId(randomIdentifier(), UUIDs.randomBase64UUID(), randomIntBetween(0, 99));
    }

    @Override
    protected ShardId mutateInstance(ShardId instance) throws IOException {
        return switch (randomInt(2)) {
            case 0 -> new ShardId(
                randomValueOtherThan(instance.getIndex().getName(), ESTestCase::randomIdentifier),
                instance.getIndex().getUUID(),
                instance.id()
            );
            case 1 -> new ShardId(
                instance.getIndex().getName(),
                randomValueOtherThan(instance.getIndex().getUUID(), UUIDs::randomBase64UUID),
                instance.id()
            );
            case 2 -> new ShardId(
                instance.getIndex().getName(),
                instance.getIndex().getUUID(),
                randomValueOtherThan(instance.id(), () -> randomIntBetween(0, 99))
            );
            default -> throw new RuntimeException("unreachable");
        };
    }

    public void testShardIdFromString() {
        String indexName = randomAlphaOfLengthBetween(3, 50);
        int shardId = randomInt();
        ShardId id = ShardId.fromString("[" + indexName + "][" + shardId + "]");
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
        ex = expectThrows(IllegalArgumentException.class, () -> ShardId.fromString(badId2));

        String badId3 = "[" + indexName + "][" + shardId; // missing closing bracket
        ex = expectThrows(IllegalArgumentException.class, () -> ShardId.fromString(badId3));
    }
}
