/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;

import java.io.IOException;
import java.util.Map;

public class GetShardSnapshotCommitInfoResponse extends ActionResponse {

    private final Map<String, BlobLocation> blobLocations;
    @Nullable
    private final String shardStateId;

    public GetShardSnapshotCommitInfoResponse(Map<String, BlobLocation> blobLocations, @Nullable String shardStateId) {
        this.blobLocations = blobLocations;
        this.shardStateId = shardStateId;
    }

    public GetShardSnapshotCommitInfoResponse(StreamInput in) throws IOException {
        this.blobLocations = in.readImmutableMap(BlobLocation::readFromTransport);
        this.shardStateId = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(blobLocations, StreamOutput::writeWriteable);
        out.writeOptionalString(shardStateId);
    }

    public Map<String, BlobLocation> blobLocations() {
        return blobLocations;
    }

    @Nullable
    public String shardStateId() {
        return shardStateId;
    }
}
