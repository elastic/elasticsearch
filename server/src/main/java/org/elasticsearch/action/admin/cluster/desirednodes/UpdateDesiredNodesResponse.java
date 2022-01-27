/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class UpdateDesiredNodesResponse extends AcknowledgedResponse {
    @Nullable
    private final String newHistoryId;

    public UpdateDesiredNodesResponse(boolean acknowledged, String newHistoryId) {
        super(acknowledged);
        this.newHistoryId = newHistoryId;
    }

    public UpdateDesiredNodesResponse(StreamInput in) throws IOException {
        super(in);
        this.newHistoryId = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(newHistoryId);
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        builder.field("new_history_id", newHistoryId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        UpdateDesiredNodesResponse that = (UpdateDesiredNodesResponse) o;
        return Objects.equals(newHistoryId, that.newHistoryId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), newHistoryId);
    }
}
