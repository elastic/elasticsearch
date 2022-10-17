/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public final class UpdateApiKeyResponse extends ActionResponse implements ToXContentObject, Writeable {
    private final boolean updated;

    public UpdateApiKeyResponse(boolean updated) {
        this.updated = updated;
    }

    public UpdateApiKeyResponse(StreamInput in) throws IOException {
        super(in);
        this.updated = in.readBoolean();
    }

    public boolean isUpdated() {
        return updated;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("updated", updated).endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(updated);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UpdateApiKeyResponse that = (UpdateApiKeyResponse) o;
        return updated == that.updated;
    }

    @Override
    public int hashCode() {
        return Objects.hash(updated);
    }
}
