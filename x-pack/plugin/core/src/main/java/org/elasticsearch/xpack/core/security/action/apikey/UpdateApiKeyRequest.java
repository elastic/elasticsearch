/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class UpdateApiKeyRequest extends BaseUpdateApiKeyRequest {

    private final String id;

    public UpdateApiKeyRequest(
        final String id,
        @Nullable final List<RoleDescriptor> roleDescriptors,
        @Nullable final Map<String, Object> metadata
    ) {
        super(roleDescriptors, metadata);
        this.id = Objects.requireNonNull(id, "API key ID must not be null");
    }

    public UpdateApiKeyRequest(StreamInput in) throws IOException {
        super(in);
        this.id = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
    }

    public static UpdateApiKeyRequest usingApiKeyId(String id) {
        return new UpdateApiKeyRequest(id, null, null);
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UpdateApiKeyRequest that = (UpdateApiKeyRequest) o;
        return id.equals(that.id) && Objects.equals(metadata, that.metadata) && Objects.equals(roleDescriptors, that.roleDescriptors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, metadata, roleDescriptors);
    }
}
