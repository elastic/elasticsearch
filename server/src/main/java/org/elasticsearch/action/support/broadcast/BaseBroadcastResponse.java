/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.broadcast;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.List;

/**
 * Base class for all broadcast operation based responses.
 */
public class BaseBroadcastResponse extends ActionResponse {

    private final int totalShards;
    private final int successfulShards;
    private final int failedShards;
    private final List<DefaultShardOperationFailedException> shardFailures;

    public BaseBroadcastResponse(StreamInput in) throws IOException {
        totalShards = in.readVInt();
        successfulShards = in.readVInt();
        failedShards = in.readVInt();
        shardFailures = in.readCollectionAsImmutableList(DefaultShardOperationFailedException::readShardOperationFailed);
    }

    public BaseBroadcastResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
        this.failedShards = failedShards;
        this.shardFailures = shardFailures == null ? List.of() : List.copyOf(shardFailures);
    }

    /**
     * The total shards this request ran against.
     */
    public int getTotalShards() {
        return totalShards;
    }

    /**
     * The successful shards this request was executed on.
     */
    public int getSuccessfulShards() {
        return successfulShards;
    }

    /**
     * The failed shards this request was executed on.
     */
    public int getFailedShards() {
        return failedShards;
    }

    /**
     * The shards that were not available when the request was executed.
     */
    public int getUnavailableShards() {
        return totalShards - successfulShards - failedShards;
    }

    /**
     * The REST status that should be used for the response
     */
    public RestStatus getStatus() {
        if (failedShards > 0) {
            return shardFailures.getFirst().status();
        } else {
            return RestStatus.OK;
        }
    }

    /**
     * The list of shard failures exception.
     */
    public List<DefaultShardOperationFailedException> getShardFailures() {
        return shardFailures;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);
        out.writeVInt(failedShards);
        out.writeCollection(shardFailures);
    }
}
