/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store.cache;

import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import static org.hamcrest.Matchers.equalTo;

public class CacheKeyTests extends ESTestCase {

    public void testEqualsAndHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createInstance(), this::copy, this::mutate);
    }

    public void testBelongsTo() {
        final CacheKey cacheKey = createInstance();

        SnapshotId snapshotId = cacheKey.getSnapshotId();
        IndexId indexId = cacheKey.getIndexId();
        ShardId shardId = cacheKey.getShardId();

        final boolean belongsTo;
        switch (randomInt(2)) {
            case 0:
                snapshotId = randomValueOtherThan(cacheKey.getSnapshotId(), this::randomSnapshotId);
                belongsTo = false;
                break;
            case 1:
                indexId = randomValueOtherThan(cacheKey.getIndexId(), this::randomIndexId);
                belongsTo = false;
                break;
            case 2:
                shardId = randomValueOtherThan(cacheKey.getShardId(), this::randomShardId);
                belongsTo = false;
                break;
            case 3:
                belongsTo = true;
                break;
            default:
                throw new AssertionError("Unsupported value");
        }

        assertThat(cacheKey.belongsTo(snapshotId, indexId, shardId), equalTo(belongsTo));
    }

    private SnapshotId randomSnapshotId() {
        return new SnapshotId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10));
    }

    private IndexId randomIndexId() {
        return new IndexId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10));
    }

    private ShardId randomShardId() {
        return new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10), randomInt(5));
    }

    private CacheKey createInstance() {
        return new CacheKey(randomSnapshotId(), randomIndexId(), randomShardId(), randomAlphaOfLengthBetween(5, 10));
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
                snapshotId = randomValueOtherThan(origin.getSnapshotId(), this::randomSnapshotId);
                break;
            case 1:
                indexId = randomValueOtherThan(origin.getIndexId(), this::randomIndexId);
                break;
            case 2:
                shardId = randomValueOtherThan(origin.getShardId(), this::randomShardId);
                break;
            case 3:
                fileName = randomValueOtherThan(origin.getFileName(), () -> randomAlphaOfLengthBetween(5, 10));
                break;
            default:
                throw new AssertionError("Unsupported mutation");
        }
        return new CacheKey(snapshotId, indexId, shardId, fileName);
    }
}
