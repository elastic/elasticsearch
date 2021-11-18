/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.shards;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class ClusterSearchShardsGroup implements Writeable, ToXContentObject {

    private final ShardId shardId;
    private final ShardRouting[] shards;

    public ClusterSearchShardsGroup(ShardId shardId, ShardRouting[] shards) {
        this.shardId = shardId;
        this.shards = shards;
    }

    ClusterSearchShardsGroup(StreamInput in) throws IOException {
        shardId = new ShardId(in);
        shards = in.readArray(i -> new ShardRouting(shardId, i), ShardRouting[]::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeArray((o, s) -> s.writeToThin(o), shards);
    }

    public ShardId getShardId() {
        return shardId;
    }

    public ShardRouting[] getShards() {
        return shards;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray();
        for (ShardRouting shard : getShards()) {
            shard.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }
}
