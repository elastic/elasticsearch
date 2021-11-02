/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.upgrade.post;

import org.elasticsearch.action.support.broadcast.BroadcastShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public final class ShardUpgradeRequest extends BroadcastShardRequest {

    private UpgradeRequest request;

    public ShardUpgradeRequest(StreamInput in) throws IOException {
        super(in);
        request = new UpgradeRequest(in);
    }

    ShardUpgradeRequest(ShardId shardId, UpgradeRequest request) {
        super(shardId, request);
        this.request = request;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }

    public UpgradeRequest upgradeRequest() {
        return this.request;
    }
}
