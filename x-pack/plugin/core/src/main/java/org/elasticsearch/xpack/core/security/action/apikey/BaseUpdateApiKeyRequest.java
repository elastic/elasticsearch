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

import static org.elasticsearch.action.ValidateActions.addValidationError;

public abstract class BaseUpdateApiKeyRequest extends ActionRequest {

    @Nullable
    protected final List<RoleDescriptor> roleDescriptors;
    @Nullable
    protected final Map<String, Object> metadata;

    public BaseUpdateApiKeyRequest(@Nullable final List<RoleDescriptor> roleDescriptors, @Nullable final Map<String, Object> metadata) {
        this.roleDescriptors = roleDescriptors;
        this.metadata = metadata;
    }

    public BaseUpdateApiKeyRequest(StreamInput in) throws IOException {
        super(in);
        this.roleDescriptors = in.readOptionalList(RoleDescriptor::new);
        this.metadata = in.readMap();
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public List<RoleDescriptor> getRoleDescriptors() {
        return roleDescriptors;
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
        out.writeOptionalCollection(roleDescriptors);
        out.writeGenericMap(metadata);
    }
}
