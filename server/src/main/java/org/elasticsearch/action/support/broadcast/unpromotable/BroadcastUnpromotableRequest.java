/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.broadcast.unpromotable;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request that is broadcast to the unpromotable assigned replicas of a primary.
 */
public class BroadcastUnpromotableRequest extends ActionRequest {

    /**
     * Holds the index shard routing table that will be used by {@link TransportBroadcastUnpromotableAction} to broadcast the requests to
     * the unpromotable replicas. The routing table is not serialized over the wire, and will be null on the other end of the wire.
     * For this reason, the field is package-private.
     */
    final @Nullable IndexShardRoutingTable indexShardRoutingTable;

    protected final ShardId primaryShardId;

    public BroadcastUnpromotableRequest(StreamInput in) throws IOException {
        super(in);
        primaryShardId = new ShardId(in);
        indexShardRoutingTable = null;
    }

    public BroadcastUnpromotableRequest(IndexShardRoutingTable indexShardRoutingTable) {
        this.indexShardRoutingTable = Objects.requireNonNull(indexShardRoutingTable, "index shard routing table is null");
        this.primaryShardId = indexShardRoutingTable.primary().shardId();
    }

    public ShardId primaryShardId() {
        return primaryShardId;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (primaryShardId == null) {
            validationException = addValidationError("primary shard is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeWriteable(primaryShardId);
    }

    @Override
    public String toString() {
        return "BroadcastUnpromotableRequest{" + "primaryShardId=" + primaryShardId() + '}';
    }

    @Override
    public String getDescription() {
        return toString();
    }

}
