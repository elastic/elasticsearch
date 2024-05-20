/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.restore;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Contains information about restores snapshot
 */
public class RestoreSnapshotResponse extends ActionResponse implements ToXContentObject {

    @Nullable
    private final RestoreInfo restoreInfo;

    public RestoreSnapshotResponse(@Nullable RestoreInfo restoreInfo) {
        this.restoreInfo = restoreInfo;
    }

    public RestoreSnapshotResponse(StreamInput in) throws IOException {
        super(in);
        restoreInfo = in.readOptionalWriteable(RestoreInfo::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(restoreInfo);
    }

    /**
     * Returns restore information if snapshot was completed before this method returned, null otherwise
     *
     * @return restore information or null
     */
    public RestoreInfo getRestoreInfo() {
        return restoreInfo;
    }

    public RestStatus status() {
        if (restoreInfo == null) {
            return RestStatus.ACCEPTED;
        }
        return RestStatus.OK;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (restoreInfo != null) {
            builder.field("snapshot");
            restoreInfo.toXContent(builder, params);
        } else {
            builder.field("accepted", true);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RestoreSnapshotResponse that = (RestoreSnapshotResponse) o;
        return Objects.equals(restoreInfo, that.restoreInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(restoreInfo);
    }

    @Override
    public String toString() {
        return "RestoreSnapshotResponse{restoreInfo=" + restoreInfo + '}';
    }
}
