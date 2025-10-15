/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class UpdateDesiredNodesResponse extends ActionResponse implements ToXContentObject {
    private static final TransportVersion DRY_RUN_SUPPORTING_VERSION = TransportVersions.V_8_4_0;

    private final boolean replacedExistingHistoryId;
    private final boolean dryRun;

    public UpdateDesiredNodesResponse(boolean replacedExistingHistoryId) {
        this(replacedExistingHistoryId, false);
    }

    public UpdateDesiredNodesResponse(boolean replacedExistingHistoryId, boolean dryRun) {
        this.replacedExistingHistoryId = replacedExistingHistoryId;
        this.dryRun = dryRun;
    }

    public UpdateDesiredNodesResponse(StreamInput in) throws IOException {
        this.replacedExistingHistoryId = in.readBoolean();
        dryRun = in.getTransportVersion().onOrAfter(DRY_RUN_SUPPORTING_VERSION) ? in.readBoolean() : false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(replacedExistingHistoryId);
        if (out.getTransportVersion().onOrAfter(DRY_RUN_SUPPORTING_VERSION)) {
            out.writeBoolean(dryRun);
        }
    }

    public boolean hasReplacedExistingHistoryId() {
        return replacedExistingHistoryId;
    }

    public boolean dryRun() {
        return dryRun;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("replaced_existing_history_id", replacedExistingHistoryId);
        builder.field("dry_run", dryRun);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UpdateDesiredNodesResponse that = (UpdateDesiredNodesResponse) o;
        return replacedExistingHistoryId == that.replacedExistingHistoryId && dryRun == that.dryRun;
    }

    @Override
    public int hashCode() {
        return Objects.hash(replacedExistingHistoryId, dryRun);
    }
}
