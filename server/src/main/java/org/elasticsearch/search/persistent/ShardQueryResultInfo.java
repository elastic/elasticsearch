/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.persistent;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public class ShardQueryResultInfo implements Writeable, Comparable<ShardQueryResultInfo> {
    private final PersistentSearchShard shardId;
    private final String nodeId;

    public ShardQueryResultInfo(PersistentSearchShard shardId, String nodeId) {
        this.shardId = shardId;
        this.nodeId = nodeId;
    }

    public ShardQueryResultInfo(StreamInput in) throws IOException {
        this.shardId = new PersistentSearchShard(in);
        this.nodeId = in.readString();
    }

    public PersistentSearchShard getShardId() {
        return shardId;
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeString(nodeId);
    }

    @Override
    public int compareTo(ShardQueryResultInfo o) {
        return shardId.compareTo(o.getShardId());
    }
}
