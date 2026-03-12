/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.action.role.RoleDescriptorRequestValidator;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.List;
import java.util.Map;

/**
 * Request class used for the creation of an API key. The request requires a name to be provided
 * and optionally an expiration time and permission limitation can be provided.
 */
public final class CreateApiKeyRequest extends AbstractCreateApiKeyRequest {

    public CreateApiKeyRequest() {
        super();
    }

    /**
     * Create API Key request constructor
     * @param name name for the API key
     * @param roleDescriptors list of {@link RoleDescriptor}s
     * @param expiration to specify expiration for the API key
     */
    public CreateApiKeyRequest(String name, @Nullable List<RoleDescriptor> roleDescriptors, @Nullable TimeValue expiration) {
        this(name, roleDescriptors, expiration, null);
    }

    public CreateApiKeyRequest(
        String name,
        @Nullable List<RoleDescriptor> roleDescriptors,
        @Nullable TimeValue expiration,
        @Nullable Map<String, Object> metadata
    ) {
        this();
        this.name = name;
        this.roleDescriptors = (roleDescriptors == null) ? List.of() : List.copyOf(roleDescriptors);
        this.expiration = expiration;
        this.metadata = metadata;
    }

    @Override
    public ApiKey.Type getType() {
        return ApiKey.Type.REST;
    }

    public void setId() {
        throw new UnsupportedOperationException("The API Key Id cannot be set, it must be generated randomly");
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setExpiration(@Nullable TimeValue expiration) {
        this.expiration = expiration;
    }

    public void setRoleDescriptors(@Nullable List<RoleDescriptor> roleDescriptors) {
        this.roleDescriptors = (roleDescriptors == null) ? List.of() : List.copyOf(roleDescriptors);
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        for (RoleDescriptor roleDescriptor : getRoleDescriptors()) {
            validationException = RoleDescriptorRequestValidator.validate(roleDescriptor, validationException);
        }
        return validationException;
    }
}
