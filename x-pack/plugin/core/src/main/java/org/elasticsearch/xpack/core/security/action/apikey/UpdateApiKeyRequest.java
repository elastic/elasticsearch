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
    private final Map<String, Object> metadata;
    private final List<RoleDescriptor> roleDescriptors;

    public UpdateApiKeyRequest(String id, List<RoleDescriptor> roleDescriptors, Map<String, Object> metadata) {
        this.id = Objects.requireNonNull(id, "api key id must not be null");
        this.roleDescriptors = (roleDescriptors == null) ? List.of() : roleDescriptors;
        this.metadata = metadata;
    }

    public UpdateApiKeyRequest(StreamInput in) throws IOException {
        super(in);
        this.id = in.readString();
        this.roleDescriptors = List.copyOf(in.readList(RoleDescriptor::new));
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
        for (RoleDescriptor roleDescriptor : roleDescriptors) {
            validationException = RoleDescriptorRequestValidator.validate(roleDescriptor, validationException);
        }
        return validationException;
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
