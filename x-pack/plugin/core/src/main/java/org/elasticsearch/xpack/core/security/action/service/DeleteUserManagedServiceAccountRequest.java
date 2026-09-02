/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.core.security.support.Validation;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DeleteUserManagedServiceAccountRequest extends UntypedActionRequest {

    private final String namespace;
    private final String serviceName;
    private WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.WAIT_UNTIL;
    private boolean force = false;

    public DeleteUserManagedServiceAccountRequest(String namespace, String serviceName) {
        this.namespace = Objects.requireNonNull(namespace, "namespace cannot be null");
        this.serviceName = Objects.requireNonNull(serviceName, "service name cannot be null");
    }

    public DeleteUserManagedServiceAccountRequest(StreamInput in) throws IOException {
        super(in);
        this.namespace = in.readString();
        this.serviceName = in.readString();
        this.refreshPolicy = WriteRequest.RefreshPolicy.readFrom(in);
        this.force = in.readBoolean();
    }

    public String getNamespace() {
        return namespace;
    }

    public String getServiceName() {
        return serviceName;
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public void setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        this.refreshPolicy = Objects.requireNonNull(refreshPolicy, "refresh policy may not be null");
    }

    public boolean isForce() {
        return force;
    }

    /**
     * When false, the default, deleting an account that still has service tokens is refused. When true the account is
     * deleted and its tokens are left in place: they cannot authenticate while no account of that name exists, but
     * creating one again later revives them.
     */
    public void setForce(boolean force) {
        this.force = force;
    }

    public ServiceAccountId getAccountId() {
        return new ServiceAccountId(namespace, serviceName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeleteUserManagedServiceAccountRequest that = (DeleteUserManagedServiceAccountRequest) o;
        return force == that.force
            && namespace.equals(that.namespace)
            && serviceName.equals(that.serviceName)
            && refreshPolicy == that.refreshPolicy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, serviceName, refreshPolicy, force);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(namespace);
        out.writeString(serviceName);
        refreshPolicy.writeTo(out);
        out.writeBoolean(force);
    }

    /**
     * A name no user-managed account could carry is rejected rather than answered with "not found", because the
     * caller cannot have created such an account and telling them it is absent hides the reason.
     */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        final Validation.Error namespaceError = Validation.UserManagedServiceAccounts.validateNamespace(namespace);
        if (namespaceError != null) {
            validationException = addValidationError(namespaceError.toString(), validationException);
        }
        final Validation.Error serviceNameError = Validation.UserManagedServiceAccounts.validateServiceName(serviceName);
        if (serviceNameError != null) {
            validationException = addValidationError(serviceNameError.toString(), validationException);
        }
        return validationException;
    }
}
