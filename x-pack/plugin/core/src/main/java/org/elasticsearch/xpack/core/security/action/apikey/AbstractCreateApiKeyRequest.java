/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public abstract class AbstractCreateApiKeyRequest extends LegacyActionRequest {
    protected final String id;
    protected String name;
    protected TimeValue expiration;
    protected Map<String, Object> metadata;
    protected List<RoleDescriptor> roleDescriptors = Collections.emptyList();
    protected WriteRequest.RefreshPolicy refreshPolicy;

    public AbstractCreateApiKeyRequest() {
        super();
        // we generate the API key id soonest so it's part of the request body so it is audited
        this.id = UUIDs.base64UUID(); // because auditing can currently only catch requests but not responses,
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public abstract ApiKey.Type getType();

    public TimeValue getExpiration() {
        return expiration;
    }

    public List<RoleDescriptor> getRoleDescriptors() {
        return roleDescriptors;
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
        assert refreshPolicy != null : "refresh policy is required";
        return validationException;
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }
}
