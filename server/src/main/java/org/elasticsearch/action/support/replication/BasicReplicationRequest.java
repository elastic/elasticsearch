/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * A replication request that has no more information than ReplicationRequest.
 * Unfortunately ReplicationRequest can't be declared as a type parameter
 * because it has a self referential type parameter of its own. So use this
 * instead.
 */
public class BasicReplicationRequest extends ReplicationRequest<BasicReplicationRequest> {
    /**
     * Creates a new request with resolved shard id
     */
    public BasicReplicationRequest(ShardId shardId) {
        super(shardId);
    }

    public BasicReplicationRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String toString() {
        return "BasicReplicationRequest{" + shardId + "}";
    }
}
