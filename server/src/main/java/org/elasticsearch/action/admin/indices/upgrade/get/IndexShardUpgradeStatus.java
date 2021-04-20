/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.upgrade.get;

import org.elasticsearch.index.shard.ShardId;

import java.util.Arrays;
import java.util.Iterator;

public class IndexShardUpgradeStatus implements Iterable<ShardUpgradeStatus> {

    private final ShardId shardId;

    private final ShardUpgradeStatus[] shards;

    IndexShardUpgradeStatus(ShardId shardId, ShardUpgradeStatus[] shards) {
        this.shardId = shardId;
        this.shards = shards;
    }

    public ShardId getShardId() {
        return this.shardId;
    }

    public ShardUpgradeStatus getAt(int i) {
        return shards[i];
    }

    public ShardUpgradeStatus[] getShards() {
        return this.shards;
    }

    @Override
    public Iterator<ShardUpgradeStatus> iterator() {
        return Arrays.stream(shards).iterator();
    }

    public long getTotalBytes() {
        long totalBytes = 0;
        for (ShardUpgradeStatus indexShardUpgradeStatus : shards) {
            totalBytes += indexShardUpgradeStatus.getTotalBytes();
        }
        return totalBytes;
    }

    public long getToUpgradeBytes() {
        long upgradeBytes = 0;
        for (ShardUpgradeStatus indexShardUpgradeStatus : shards) {
            upgradeBytes += indexShardUpgradeStatus.getToUpgradeBytes();
        }
        return upgradeBytes;
    }

    public long getToUpgradeBytesAncient() {
        long upgradeBytesAncient = 0;
        for (ShardUpgradeStatus indexShardUpgradeStatus : shards) {
            upgradeBytesAncient += indexShardUpgradeStatus.getToUpgradeBytesAncient();
        }
        return upgradeBytesAncient;
    }
}
