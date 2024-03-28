/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.Version;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * A replication request that has no more information than ReplicationRequest.
 * Unfortunately ReplicationRequest can't be declared as a type parameter
 * because it has a self referential type parameter of its own. So use this
 * instead.
 */
public class BasicReplicationRequest extends ReplicationRequest<BasicReplicationRequest> {

    private final OriginalIndices originalIndices;

    /**
     * Creates a new request with resolved shard id
     */
    public BasicReplicationRequest(ShardId shardId, IndicesRequest request) {
        super(shardId);
        this.originalIndices = new OriginalIndices(request);
    }

    public BasicReplicationRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_8_2_0) && in.readBoolean()) {
            this.originalIndices = OriginalIndices.readOriginalIndices(in);
        } else {
            this.originalIndices = null;
        }
    }

    @Override
    public String[] indices() {
        return originalIndices != null ? originalIndices.indices() : super.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return originalIndices != null ? originalIndices.indicesOptions() : super.indicesOptions();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_8_2_0)) {
            if (originalIndices != null) {
                out.writeBoolean(true);
                OriginalIndices.writeOriginalIndices(originalIndices, out);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    @Override
    public String toString() {
        return "BasicReplicationRequest{" + "originalIndices=" + originalIndices + ", shardId=" + shardId + '}';
    }
}
