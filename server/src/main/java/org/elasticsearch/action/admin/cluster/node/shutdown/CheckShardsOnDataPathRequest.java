/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

public class CheckShardsOnDataPathRequest extends BaseNodesRequest<CheckShardsOnDataPathRequest> {

    private final Set<ShardId> shardIds;
    @Nullable
    private final String customDataPath;

    public CheckShardsOnDataPathRequest(Set<ShardId> shardIds, String customDataPath, String... nodeIds) {
        super(nodeIds);
        this.shardIds = Set.copyOf(Objects.requireNonNull(shardIds));
        this.customDataPath = Objects.requireNonNull(customDataPath);
    }

    public CheckShardsOnDataPathRequest(StreamInput in) throws IOException {
        super(in);
        this.shardIds = in.readSet(ShardId::new);
        this.customDataPath = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeCollection(shardIds);
        out.writeString(customDataPath);
    }

    public Set<ShardId> getShardIds() {
        return shardIds;
    }

    public String getCustomDataPath() {
        return customDataPath;
    }
}
