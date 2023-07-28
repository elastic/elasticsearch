/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.action.role.RoleDescriptorRequestValidator;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    public CreateApiKeyRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_7_5_0)) {
            this.name = in.readOptionalString();
        } else {
            this.name = in.readString();
        }
        this.expiration = in.readOptionalTimeValue();
        this.roleDescriptors = in.readImmutableList(RoleDescriptor::new);
        this.refreshPolicy = WriteRequest.RefreshPolicy.readFrom(in);
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_0_0)) {
            this.metadata = in.readMap();
        } else {
            this.metadata = null;
        }
    }

    @Override
    protected String doReadId(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_7_10_0)) {
            return in.readString();
        } else {
            return UUIDs.base64UUID();
        }
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

    public void setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        this.refreshPolicy = Objects.requireNonNull(refreshPolicy, "refresh policy may not be null");
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_10_0)) {
            out.writeString(id);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_5_0)) {
            out.writeOptionalString(name);
        } else {
            out.writeString(name);
        }
        out.writeOptionalTimeValue(expiration);
        out.writeList(getRoleDescriptors());
        refreshPolicy.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_13_0)) {
            out.writeGenericMap(metadata);
        }
    }
}
