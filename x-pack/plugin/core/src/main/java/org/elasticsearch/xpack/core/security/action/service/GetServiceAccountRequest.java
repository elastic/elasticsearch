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
import org.elasticsearch.xpack.core.security.support.ManagedServiceAccountIdValidator;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetServiceAccountRequest extends UntypedActionRequest {

    @Nullable
    private final String namespace;
    @Nullable
    private final String serviceName;
    private final boolean includeManaged;

    public GetServiceAccountRequest(@Nullable String namespace, @Nullable String serviceName) {
        this(namespace, serviceName, false);
    }

    public GetServiceAccountRequest(@Nullable String namespace, @Nullable String serviceName, boolean includeManaged) {
        this.namespace = namespace;
        this.serviceName = serviceName;
        this.includeManaged = includeManaged;
    }

    public GetServiceAccountRequest(StreamInput in) throws IOException {
        super(in);
        this.namespace = in.readOptionalString();
        this.serviceName = in.readOptionalString();
        if (in.getTransportVersion().supports(ServiceAccountInfo.MANAGED_SERVICE_ACCOUNTS)) {
            includeManaged = in.readBoolean();
        } else {
            includeManaged = false;
        }
    }

    public String getNamespace() {
        return namespace;
    }

    public String getServiceName() {
        return serviceName;
    }

    public boolean isIncludeManaged() {
        return includeManaged;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetServiceAccountRequest that = (GetServiceAccountRequest) o;
        return includeManaged == that.includeManaged
            && Objects.equals(namespace, that.namespace)
            && Objects.equals(serviceName, that.serviceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, serviceName, includeManaged);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(namespace);
        out.writeOptionalString(serviceName);
        if (out.getTransportVersion().supports(ServiceAccountInfo.MANAGED_SERVICE_ACCOUNTS)) {
            out.writeBoolean(includeManaged);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (namespace != null && ManagedServiceAccountIdValidator.BUILTIN_NAMESPACE.equals(namespace) == false) {
            final String namespaceError = ManagedServiceAccountIdValidator.validateNamespace(namespace);
            if (namespaceError != null) {
                validationException = addValidationError(namespaceError, validationException);
            }
        }
        if (serviceName != null) {
            final String serviceNameError = ManagedServiceAccountIdValidator.validateServiceName(serviceName);
            if (serviceNameError != null) {
                validationException = addValidationError(serviceNameError, validationException);
            }
        }
        return validationException;
    }
}
