/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetServiceAccountRequest extends UntypedActionRequest {

    @Nullable
    private final String namespace;
    @Nullable
    private final String serviceName;
    private final EnumSet<ServiceAccountManagedBy> managedBy;

    public GetServiceAccountRequest(@Nullable String namespace, @Nullable String serviceName) {
        this(namespace, serviceName, EnumSet.of(ServiceAccountManagedBy.ELASTIC));
    }

    public GetServiceAccountRequest(@Nullable String namespace, @Nullable String serviceName, EnumSet<ServiceAccountManagedBy> managedBy) {
        this.namespace = namespace;
        this.serviceName = serviceName;
        this.managedBy = EnumSet.copyOf(Objects.requireNonNull(managedBy, "managed_by cannot be null"));
    }

    public GetServiceAccountRequest(StreamInput in) throws IOException {
        super(in);
        this.namespace = in.readOptionalString();
        this.serviceName = in.readOptionalString();
        if (in.getTransportVersion().supports(ServiceAccountInfo.MANAGED_SERVICE_ACCOUNTS)) {
            managedBy = in.readEnumSet(ServiceAccountManagedBy.class);
        } else {
            managedBy = EnumSet.of(ServiceAccountManagedBy.ELASTIC);
        }
    }

    public String getNamespace() {
        return namespace;
    }

    public String getServiceName() {
        return serviceName;
    }

    public EnumSet<ServiceAccountManagedBy> getManagedBy() {
        return managedBy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetServiceAccountRequest that = (GetServiceAccountRequest) o;
        return managedBy.equals(that.managedBy)
            && Objects.equals(namespace, that.namespace)
            && Objects.equals(serviceName, that.serviceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, serviceName, managedBy);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(namespace);
        out.writeOptionalString(serviceName);
        if (out.getTransportVersion().supports(ServiceAccountInfo.MANAGED_SERVICE_ACCOUNTS)) {
            out.writeEnumSet(managedBy);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (managedBy.isEmpty()) {
            validationException = addValidationError("managed_by must contain at least one value", validationException);
        }
        return validationException;
    }
}
