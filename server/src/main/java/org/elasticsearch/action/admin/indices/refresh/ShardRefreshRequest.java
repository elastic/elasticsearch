/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class ShardRefreshRequest extends ReplicationRequest<ShardRefreshRequest> {

    private final RefreshRequest request;

    public ShardRefreshRequest(RefreshRequest request, ShardId shardId) {
        super(shardId);
        this.request = request;
        this.waitForActiveShards = ActiveShardCount.NONE; // don't wait for any active shards before proceeding, by default
    }

    public ShardRefreshRequest(StreamInput in) throws IOException {
        super(in);
        request = new RefreshRequest(in);
    }

    RefreshRequest getRequest() {
        return request;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }

    @Override
    public String toString() {
        return "refresh {" + shardId + "}";
    }
}
