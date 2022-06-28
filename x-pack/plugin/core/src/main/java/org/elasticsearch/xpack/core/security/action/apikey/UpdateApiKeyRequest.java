/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.action.role.RoleDescriptorRequestValidator;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public final class UpdateApiKeyRequest extends ActionRequest {

    private final String id;
    @Nullable
    private final Map<String, Object> metadata;
    @Nullable
    private final List<RoleDescriptor> roleDescriptors;

    public UpdateApiKeyRequest(String id, @Nullable List<RoleDescriptor> roleDescriptors, @Nullable Map<String, Object> metadata) {
        this.id = Objects.requireNonNull(id, "API key ID must not be null");
        this.roleDescriptors = roleDescriptors;
        this.metadata = metadata;
    }

    public UpdateApiKeyRequest(StreamInput in) throws IOException {
        super(in);
        this.id = in.readString();
        this.roleDescriptors = readOptionalList(in);
        this.metadata = in.readMap();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (metadata != null && MetadataUtils.containsReservedMetadata(metadata)) {
            validationException = addValidationError(
                "API key metadata keys may not start with [" + MetadataUtils.RESERVED_PREFIX + "]",
                validationException
            );
        }
        if (roleDescriptors != null) {
            for (RoleDescriptor roleDescriptor : roleDescriptors) {
                validationException = RoleDescriptorRequestValidator.validate(roleDescriptor, validationException);
            }
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        writeOptionalList(out);
        out.writeGenericMap(metadata);
    }

    private List<RoleDescriptor> readOptionalList(StreamInput in) throws IOException {
        return in.readBoolean() ? in.readList(RoleDescriptor::new) : null;
    }

    private void writeOptionalList(StreamOutput out) throws IOException {
        if (roleDescriptors == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeList(roleDescriptors);
        }
    }

    public String getId() {
        return id;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public List<RoleDescriptor> getRoleDescriptors() {
        return roleDescriptors;
    }
}
