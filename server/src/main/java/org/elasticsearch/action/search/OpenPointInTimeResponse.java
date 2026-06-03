/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

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
    private final SearchResponse.Clusters clusters;

    public OpenPointInTimeResponse(
        BytesReference pointInTimeId,
        int totalShards,
        int successfulShards,
        int failedShards,
        int skippedShards,
        SearchResponse.Clusters clusters
    ) {
        this.pointInTimeId = Objects.requireNonNull(pointInTimeId, "Point in time parameter must be not null");
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
        this.failedShards = failedShards;
        this.skippedShards = skippedShards;
        this.clusters = Objects.requireNonNull(clusters);
    }

    /**
     * This response is always delivered as an in-memory object via {@code NodeClient#executeLocally};
     * the REST layer then renders it as XContent through {@link #toXContent}.
     * If a transport path is ever needed, this method must be updated together with a new
     * {@code TransportVersion} constant and a matching {@code StreamInput} constructor in the same
     * change.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBytesReference(pointInTimeId);
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);
        out.writeVInt(failedShards);
        out.writeVInt(skippedShards);
        clusters.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(pointInTimeId)));
        buildBroadcastShardsHeader(builder, params, totalShards, successfulShards, skippedShards, failedShards, null);
        if (clusters.hasRemoteClusters()) {
            clusters.toXContent(builder, params);
        }
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

    public SearchResponse.Clusters getClusters() {
        return clusters;
    }
}
