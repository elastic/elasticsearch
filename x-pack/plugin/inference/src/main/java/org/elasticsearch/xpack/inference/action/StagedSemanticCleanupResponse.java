/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Response for the {@code POST /{index}/_semantic_cleanup} endpoint.
 *
 * <p>Extends the standard shard broadcast fields with {@code cleared} (number of
 * staged fields successfully removed) and {@code failed} (number of per-field
 * cleanup failures across all shards).
 */
public class StagedSemanticCleanupResponse extends BroadcastResponse {

    private final int cleared;
    private final int failed;

    public StagedSemanticCleanupResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures,
        int cleared,
        int failed
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.cleared = cleared;
        this.failed = failed;
    }

    public StagedSemanticCleanupResponse(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().supports(StagedSemanticCleanupRequest.STAGED_SEMANTIC_CLEANUP_FIELDS_ADDED)) {
            this.cleared = in.readVInt();
            this.failed = in.readVInt();
        } else {
            this.cleared = 0;
            this.failed = 0;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().supports(StagedSemanticCleanupRequest.STAGED_SEMANTIC_CLEANUP_FIELDS_ADDED)) {
            out.writeVInt(cleared);
            out.writeVInt(failed);
        }
    }

    /** Returns the number of staged fields cleared across all shards. */
    public int cleared() {
        return cleared;
    }

    /** Returns the number of per-field cleanup failures across all shards. */
    public int failed() {
        return failed;
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        builder.field("cleared", cleared);
        builder.field("failed", failed);
    }
}
