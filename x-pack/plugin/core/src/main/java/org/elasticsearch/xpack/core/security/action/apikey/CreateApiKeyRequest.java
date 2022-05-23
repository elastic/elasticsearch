/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.action.role.RoleDescriptorRequestValidator;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request class used for the creation of an API key. The request requires a name to be provided
 * and optionally an expiration time and permission limitation can be provided.
 */
public final class CreateApiKeyRequest extends ActionRequest {
    public static final WriteRequest.RefreshPolicy DEFAULT_REFRESH_POLICY = WriteRequest.RefreshPolicy.WAIT_UNTIL;

    private final String id;
    private String name;
    private TimeValue expiration;
    private Map<String, Object> metadata;
    private List<RoleDescriptor> roleDescriptors = Collections.emptyList();
    private WriteRequest.RefreshPolicy refreshPolicy = DEFAULT_REFRESH_POLICY;

    public CreateApiKeyRequest() {
        super();
        this.id = UUIDs.base64UUID(); // because auditing can currently only catch requests but not responses,
        // we generate the API key id soonest so it's part of the request body so it is audited
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
        if (in.getVersion().onOrAfter(Version.V_7_10_0)) {
            this.id = in.readString();
        } else {
            this.id = UUIDs.base64UUID();
        }
        if (in.getVersion().onOrAfter(Version.V_7_5_0)) {
            this.name = in.readOptionalString();
        } else {
            this.name = in.readString();
        }
        this.expiration = in.readOptionalTimeValue();
        this.roleDescriptors = List.copyOf(in.readList(RoleDescriptor::new));
        this.refreshPolicy = WriteRequest.RefreshPolicy.readFrom(in);
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            this.metadata = in.readMap();
        } else {
            this.metadata = null;
        }
    }

    public String getId() {
        return id;
    }

    public void setId() {
        throw new UnsupportedOperationException("The API Key Id cannot be set, it must be generated randomly");
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TimeValue getExpiration() {
        return expiration;
    }

    public void setExpiration(@Nullable TimeValue expiration) {
        this.expiration = expiration;
    }

    public List<RoleDescriptor> getRoleDescriptors() {
        return roleDescriptors;
    }

    public void setRoleDescriptors(@Nullable List<RoleDescriptor> roleDescriptors) {
        this.roleDescriptors = (roleDescriptors == null) ? List.of() : List.copyOf(roleDescriptors);
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public void setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        this.refreshPolicy = Objects.requireNonNull(refreshPolicy, "refresh policy may not be null");
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isNullOrEmpty(name)) {
            validationException = addValidationError("api key name is required", validationException);
        } else {
            if (name.length() > 256) {
                validationException = addValidationError("api key name may not be more than 256 characters long", validationException);
            }
            if (name.equals(name.trim()) == false) {
                validationException = addValidationError("api key name may not begin or end with whitespace", validationException);
            }
            if (name.startsWith("_")) {
                validationException = addValidationError("api key name may not begin with an underscore", validationException);
            }
        }
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_7_10_0)) {
            out.writeString(id);
        }
        if (out.getVersion().onOrAfter(Version.V_7_5_0)) {
            out.writeOptionalString(name);
        } else {
            out.writeString(name);
        }
        out.writeOptionalTimeValue(expiration);
        out.writeList(roleDescriptors);
        refreshPolicy.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_7_13_0)) {
            out.writeGenericMap(metadata);
        }
    }
}
