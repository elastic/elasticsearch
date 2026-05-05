/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutServiceAccountRequest extends LegacyActionRequest {

    private static final Pattern VALID_NAMESPACE_OR_SERVICE = Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$");

    private final String namespace;
    private final String serviceName;
    private final RoleDescriptor roleDescriptor;
    private WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.WAIT_UNTIL;

    public PutServiceAccountRequest(String namespace, String serviceName, RoleDescriptor roleDescriptor) {
        this.namespace = namespace;
        this.serviceName = serviceName;
        this.roleDescriptor = roleDescriptor;
    }

    public PutServiceAccountRequest(StreamInput in) throws IOException {
        super(in);
        this.namespace = in.readString();
        this.serviceName = in.readString();
        this.roleDescriptor = new RoleDescriptor(in);
        this.refreshPolicy = WriteRequest.RefreshPolicy.readFrom(in);
    }

    public String getNamespace() {
        return namespace;
    }

    public String getServiceName() {
        return serviceName;
    }

    public RoleDescriptor getRoleDescriptor() {
        return roleDescriptor;
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public void setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        this.refreshPolicy = Objects.requireNonNull(refreshPolicy, "refresh policy may not be null");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(namespace);
        out.writeString(serviceName);
        roleDescriptor.writeTo(out);
        refreshPolicy.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isNullOrEmpty(namespace)) {
            validationException = addValidationError("service account namespace is required", validationException);
        } else if (false == VALID_NAMESPACE_OR_SERVICE.matcher(namespace).matches()) {
            validationException = addValidationError(
                "service account namespace ["
                    + namespace
                    + "] must start with an alphanumeric character and contain only alphanumeric characters, "
                    + "underscores, hyphens, or periods (max 64 characters)",
                validationException
            );
        }
        if (Strings.isNullOrEmpty(serviceName)) {
            validationException = addValidationError("service account service-name is required", validationException);
        } else if (false == VALID_NAMESPACE_OR_SERVICE.matcher(serviceName).matches()) {
            validationException = addValidationError(
                "service account service-name ["
                    + serviceName
                    + "] must start with an alphanumeric character and contain only alphanumeric characters, "
                    + "underscores, hyphens, or periods (max 64 characters)",
                validationException
            );
        }
        if (roleDescriptor == null) {
            validationException = addValidationError("role_descriptor is required", validationException);
        }
        return validationException;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PutServiceAccountRequest that = (PutServiceAccountRequest) o;
        return Objects.equals(namespace, that.namespace)
            && Objects.equals(serviceName, that.serviceName)
            && Objects.equals(roleDescriptor, that.roleDescriptor)
            && refreshPolicy == that.refreshPolicy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, serviceName, roleDescriptor, refreshPolicy);
    }
}
