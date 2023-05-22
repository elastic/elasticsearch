/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public final class UpdateCrossClusterApiKeyRequest extends BaseUpdateApiKeyRequest {
    private final String id;

    public UpdateCrossClusterApiKeyRequest(
        final String id,
        @Nullable CrossClusterApiKeyRoleDescriptorBuilder roleDescriptorBuilder,
        @Nullable final Map<String, Object> metadata
    ) {
        super(roleDescriptorBuilder == null ? null : List.of(roleDescriptorBuilder.build()), metadata);
        this.id = Objects.requireNonNull(id, "API key ID must not be null");
    }

    public UpdateCrossClusterApiKeyRequest(StreamInput in) throws IOException {
        super(in);
        this.id = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
    }

    public String getId() {
        return id;
    }

    @Override
    public ApiKey.Type getType() {
        return ApiKey.Type.CROSS_CLUSTER;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (roleDescriptors == null && metadata == null) {
            validationException = addValidationError(
                "must update either [access] or [metadata] for cross-cluster API keys",
                validationException
            );
        }
        return validationException;
    }
}
