/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

public class CacheKeyTests extends ESTestCase {

    public void testEqualsAndHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createInstance(), this::copy, this::mutate);
    }

    private CacheKey createInstance() {
        return new CacheKey(
            new SnapshotId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10)),
            new IndexId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10)),
            new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10), randomInt(5)),
            randomAlphaOfLengthBetween(5, 10)
        );
    }

    private CacheKey copy(final CacheKey origin) {
        SnapshotId snapshotId = origin.getSnapshotId();
        if (randomBoolean()) {
            snapshotId = new SnapshotId(origin.getSnapshotId().getName(), origin.getSnapshotId().getUUID());
        }
        IndexId indexId = origin.getIndexId();
        if (randomBoolean()) {
            indexId = new IndexId(origin.getIndexId().getName(), origin.getIndexId().getId());
        }
        ShardId shardId = origin.getShardId();
        if (randomBoolean()) {
            shardId = new ShardId(new Index(shardId.getIndex().getName(), shardId.getIndex().getUUID()), shardId.id());
        }
        return new CacheKey(snapshotId, indexId, shardId, origin.getFileName());
    }

    private CacheKey mutate(CacheKey origin) {
        SnapshotId snapshotId = origin.getSnapshotId();
        IndexId indexId = origin.getIndexId();
        ShardId shardId = origin.getShardId();
        String fileName = origin.getFileName();

        switch (randomInt(3)) {
            case 0:
                snapshotId = new SnapshotId(randomAlphaOfLength(4), randomAlphaOfLength(4));
                break;
            case 1:
                indexId = new IndexId(randomAlphaOfLength(4), randomAlphaOfLength(4));
                break;
            case 2:
                shardId = new ShardId(randomAlphaOfLength(4), randomAlphaOfLength(4), randomIntBetween(6, 10));
                break;
            case 3:
                fileName = randomAlphaOfLength(15);
                break;
            default:
                throw new AssertionError("Unsupported mutation");
        }
        return new CacheKey(snapshotId, indexId, shardId, fileName);
    }

}
