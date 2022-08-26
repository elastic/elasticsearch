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
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public final class BulkUpdateApiKeyRequest extends BaseUpdateApiKeyRequest {

    public static BulkUpdateApiKeyRequest usingApiKeyIds(String... ids) {
        return new BulkUpdateApiKeyRequest(Arrays.stream(ids).toList(), null, null);
    }

    public static BulkUpdateApiKeyRequest wrap(final UpdateApiKeyRequest request) {
        return new BulkUpdateApiKeyRequest(List.of(request.getId()), request.getRoleDescriptors(), request.getMetadata());
    }

    private final List<String> ids;

    public BulkUpdateApiKeyRequest(
        final List<String> ids,
        @Nullable final List<RoleDescriptor> roleDescriptors,
        @Nullable final Map<String, Object> metadata
    ) {
        super(roleDescriptors, metadata);
        this.ids = Objects.requireNonNull(ids, "API key IDs must not be null");
    }

    public BulkUpdateApiKeyRequest(StreamInput in) throws IOException {
        super(in);
        this.ids = in.readStringList();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (ids.isEmpty()) {
            validationException = addValidationError("Field [ids] cannot be empty", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringCollection(ids);
    }

    public List<String> getIds() {
        return ids;
    }
}
