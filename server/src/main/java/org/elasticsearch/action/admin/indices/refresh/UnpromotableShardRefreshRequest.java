/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.broadcast.unpromotable.BroadcastUnpromotableRequest;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.Engine;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class UnpromotableShardRefreshRequest extends BroadcastUnpromotableRequest {

    private final long primaryTerm;
    private final long segmentGeneration;
    private final TimeValue timeout;

    public UnpromotableShardRefreshRequest(
        IndexShardRoutingTable indexShardRoutingTable,
        long primaryTerm,
        long segmentGeneration,
        boolean failShardOnError
    ) {
        this(indexShardRoutingTable, primaryTerm, segmentGeneration, failShardOnError, null);
    }

    public UnpromotableShardRefreshRequest(
        IndexShardRoutingTable indexShardRoutingTable,
        long primaryTerm,
        long segmentGeneration,
        boolean failShardOnError,
        @Nullable TimeValue timeout
    ) {
        super(indexShardRoutingTable, failShardOnError);
        this.primaryTerm = primaryTerm;
        this.segmentGeneration = segmentGeneration;
        this.timeout = timeout;
    }

    public UnpromotableShardRefreshRequest(StreamInput in) throws IOException {
        super(in);
        segmentGeneration = in.readVLong();
        primaryTerm = in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0) ? in.readVLong() : Engine.UNKNOWN_PRIMARY_TERM;
        // The timeout is only used by the request sender, therefore we don't write it over the wire
        timeout = null;
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
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeVLong(primaryTerm);
        }
    }

    public long getSegmentGeneration() {
        return segmentGeneration;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    @Nullable
    public TimeValue getTimeout() {
        return timeout;
    }

    @Override
    public String toString() {
        return Strings.format(
            "UnpromotableShardRefreshRequest{shardId=%s, primaryTerm=%d, segmentGeneration=%d}",
            shardId(),
            primaryTerm,
            segmentGeneration
        );
    }
}
