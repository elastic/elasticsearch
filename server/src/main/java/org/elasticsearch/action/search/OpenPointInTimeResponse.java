/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Base64;
import java.util.Objects;

import static org.elasticsearch.rest.action.RestActions.buildBroadcastShardsHeader;

public final class OpenPointInTimeResponse extends ActionResponse implements ToXContentObject {
    private final BytesReference pointInTimeId;

    private final int totalShards;
    private final int successfulShards;
    private final int failedShards;
    private final int skippedShards;

    public OpenPointInTimeResponse(
        BytesReference pointInTimeId,
        int totalShards,
        int successfulShards,
        int failedShards,
        int skippedShards
    ) {
        this.pointInTimeId = Objects.requireNonNull(pointInTimeId, "Point in time parameter must be not null");
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
        this.failedShards = failedShards;
        this.skippedShards = skippedShards;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBytesReference(pointInTimeId);
        if (out.getTransportVersion().onOrAfter(TransportVersions.ALLOW_PARTIAL_SEARCH_RESULTS_IN_PIT)) {
            out.writeVInt(totalShards);
            out.writeVInt(successfulShards);
            out.writeVInt(failedShards);
            out.writeVInt(skippedShards);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(pointInTimeId)));
        buildBroadcastShardsHeader(builder, params, totalShards, successfulShards, failedShards, skippedShards, null);
        builder.endObject();
        return builder;
    }

    public BytesReference getPointInTimeId() {
        return pointInTimeId;
    }

    public int getTotalShards() {
        return totalShards;
    }

    public int getSuccessfulShards() {
        return successfulShards;
    }

    public int getFailedShards() {
        return failedShards;
    }

    public int getSkippedShards() {
        return skippedShards;
    }
}
