/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class SetResetModeActionRequest extends AcknowledgedRequest<SetResetModeActionRequest> implements ToXContentObject {

    public static SetResetModeActionRequest enabled(TimeValue masterNodeTimeout) {
        return new SetResetModeActionRequest(masterNodeTimeout, true, false);
    }

    public static SetResetModeActionRequest disabled(TimeValue masterNodeTimeout, boolean deleteMetadata) {
        return new SetResetModeActionRequest(masterNodeTimeout, false, deleteMetadata);
    }

    private final boolean enabled;
    private final boolean deleteMetadata;

    static final String ENABLED_FIELD_NAME = "enabled";
    static final String DELETE_METADATA_FIELD_NAME = "delete_metadata";

    SetResetModeActionRequest(TimeValue masterNodeTimeout, boolean enabled, Boolean deleteMetadata) {
        super(masterNodeTimeout, MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT /* wait indefinitely for acks */);
        this.enabled = enabled;
        this.deleteMetadata = deleteMetadata != null && deleteMetadata;
    }

    public SetResetModeActionRequest(StreamInput in) throws IOException {
        super(in);
        this.enabled = in.readBoolean();
        this.deleteMetadata = in.readBoolean();
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean shouldDeleteMetadata() {
        return deleteMetadata;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(enabled);
        out.writeBoolean(deleteMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(masterNodeTimeout(), enabled, deleteMetadata);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        SetResetModeActionRequest other = (SetResetModeActionRequest) obj;
        return Objects.equals(masterNodeTimeout(), other.masterNodeTimeout())
            && Objects.equals(enabled, other.enabled)
            && Objects.equals(deleteMetadata, other.deleteMetadata);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(ENABLED_FIELD_NAME, enabled);
        if (enabled == false) {
            builder.field(DELETE_METADATA_FIELD_NAME, deleteMetadata);
        }
        builder.endObject();
        return builder;
    }
}
