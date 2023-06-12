/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.broadcast.unpromotable.BroadcastUnpromotableRequest;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.engine.Engine;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class UnpromotableShardRefreshRequest extends BroadcastUnpromotableRequest {

    private final long segmentGeneration;

    public UnpromotableShardRefreshRequest(
        IndexShardRoutingTable indexShardRoutingTable,
        long segmentGeneration,
        boolean failShardOnError
    ) {
        super(indexShardRoutingTable, failShardOnError);
        this.segmentGeneration = segmentGeneration;
    }

    public UnpromotableShardRefreshRequest(StreamInput in) throws IOException {
        super(in);
        segmentGeneration = in.readVLong();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (segmentGeneration == Engine.RefreshResult.UNKNOWN_GENERATION) {
            validationException = addValidationError("segment generation is unknown", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(segmentGeneration);
    }

    public long getSegmentGeneration() {
        return segmentGeneration;
    }

    @Override
    public String toString() {
        return "UnpromotableShardRefreshRequest{" + "shardId=" + shardId() + ", segmentGeneration=" + segmentGeneration + '}';
    }
}
