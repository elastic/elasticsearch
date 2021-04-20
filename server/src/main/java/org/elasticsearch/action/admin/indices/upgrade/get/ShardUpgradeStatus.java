/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.upgrade.get;

import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class ShardUpgradeStatus extends BroadcastShardResponse {

    private ShardRouting shardRouting;

    private long totalBytes;

    private long toUpgradeBytes;

    private long toUpgradeBytesAncient;

    public ShardUpgradeStatus(StreamInput in) throws IOException {
        super(in);
        shardRouting = new ShardRouting(in);
        totalBytes = in.readLong();
        toUpgradeBytes = in.readLong();
        toUpgradeBytesAncient = in.readLong();
    }

    ShardUpgradeStatus(ShardRouting shardRouting, long totalBytes, long toUpgradeBytes, long upgradeBytesAncient) {
        super(shardRouting.shardId());
        this.shardRouting = shardRouting;
        this.totalBytes = totalBytes;
        this.toUpgradeBytes = toUpgradeBytes;
        this.toUpgradeBytesAncient = upgradeBytesAncient;

    }

    public ShardRouting getShardRouting() {
        return this.shardRouting;
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    public long getToUpgradeBytes() {
        return toUpgradeBytes;
    }

    public long getToUpgradeBytesAncient() {
        return toUpgradeBytesAncient;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardRouting.writeTo(out);
        out.writeLong(totalBytes);
        out.writeLong(toUpgradeBytes);
        out.writeLong(toUpgradeBytesAncient);
    }
}
