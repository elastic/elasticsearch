/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.common;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.Locale;

public class CacheKeyTests extends ESTestCase {

    public void testEqualsAndHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createInstance(), this::copy, this::mutate);
    }

    private String randomSnapshotUUID() {
        return UUIDs.randomBase64UUID(random());
    }

    private String randomSnapshotIndexName() {
        return randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT);
    }

    private ShardId randomShardId() {
        return new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10), randomInt(5));
    }

    private CacheKey createInstance() {
        return new CacheKey(randomSnapshotUUID(), randomSnapshotIndexName(), randomShardId(), randomAlphaOfLengthBetween(5, 10));
    }

    private CacheKey copy(final CacheKey origin) {
        ShardId shardId = origin.shardId();
        if (randomBoolean()) {
            shardId = new ShardId(new Index(shardId.getIndex().getName(), shardId.getIndex().getUUID()), shardId.id());
        }
        return new CacheKey(origin.snapshotUUID(), origin.snapshotIndexName(), shardId, origin.fileName());
    }

    private CacheKey mutate(CacheKey origin) {
        String snapshotUUID = origin.snapshotUUID();
        String snapshotIndexName = origin.snapshotIndexName();
        ShardId shardId = origin.shardId();
        String fileName = origin.fileName();

        switch (randomInt(3)) {
            case 0 -> snapshotUUID = randomValueOtherThan(snapshotUUID, this::randomSnapshotUUID);
            case 1 -> snapshotIndexName = randomValueOtherThan(snapshotIndexName, this::randomSnapshotIndexName);
            case 2 -> shardId = randomValueOtherThan(origin.shardId(), this::randomShardId);
            case 3 -> fileName = randomValueOtherThan(origin.fileName(), () -> randomAlphaOfLengthBetween(5, 10));
            default -> throw new AssertionError("Unsupported mutation");
        }
        return new CacheKey(snapshotUUID, snapshotIndexName, shardId, fileName);
    }
}
