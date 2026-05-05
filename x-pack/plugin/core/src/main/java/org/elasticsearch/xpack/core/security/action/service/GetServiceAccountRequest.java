/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Objects;

public class GetServiceAccountRequest extends LegacyActionRequest {

    private static final TransportVersion USER_DEFINED_SERVICE_ACCOUNTS = TransportVersion.fromName("user_defined_service_accounts");

    @Nullable
    private final String namespace;
    @Nullable
    private final String serviceName;
    private final boolean includeUserDefined;

    public GetServiceAccountRequest(@Nullable String namespace, @Nullable String serviceName) {
        this(namespace, serviceName, false);
    }

    public GetServiceAccountRequest(@Nullable String namespace, @Nullable String serviceName, boolean includeUserDefined) {
        this.namespace = namespace;
        this.serviceName = serviceName;
        this.includeUserDefined = includeUserDefined;
    }

    public GetServiceAccountRequest(StreamInput in) throws IOException {
        super(in);
        this.namespace = in.readOptionalString();
        this.serviceName = in.readOptionalString();
        if (in.getTransportVersion().supports(USER_DEFINED_SERVICE_ACCOUNTS)) {
            this.includeUserDefined = in.readBoolean();
        } else {
            this.includeUserDefined = false;
        }
    }

    public String getNamespace() {
        return namespace;
    }

    public String getServiceName() {
        return serviceName;
    }

    public boolean isIncludeUserDefined() {
        return includeUserDefined;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetServiceAccountRequest that = (GetServiceAccountRequest) o;
        return includeUserDefined == that.includeUserDefined
            && Objects.equals(namespace, that.namespace)
            && Objects.equals(serviceName, that.serviceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, serviceName, includeUserDefined);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(namespace);
        out.writeOptionalString(serviceName);
        if (out.getTransportVersion().supports(USER_DEFINED_SERVICE_ACCOUNTS)) {
            out.writeBoolean(includeUserDefined);
        }
        // On older nodes the flag is silently dropped: those nodes don't know about user-defined
        // service accounts, so they'd return only built-ins anyway.
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
