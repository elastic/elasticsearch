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
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount;
import org.elasticsearch.xpack.core.security.support.ManagedServiceAccountIdValidator;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DeleteManagedServiceAccountRequest extends UntypedActionRequest {

    private final String namespace;
    private final String serviceName;
    private WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.WAIT_UNTIL;
    private boolean force = false;

    public DeleteManagedServiceAccountRequest(String namespace, String serviceName) {
        this.namespace = Objects.requireNonNull(namespace, "namespace cannot be null");
        this.serviceName = Objects.requireNonNull(serviceName, "service name cannot be null");
    }

    public DeleteManagedServiceAccountRequest(StreamInput in) throws IOException {
        super(in);
        namespace = in.readString();
        serviceName = in.readString();
        refreshPolicy = WriteRequest.RefreshPolicy.readFrom(in);
        force = in.readBoolean();
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
        this.refreshPolicy = refreshPolicy;
    }

    public boolean isForce() {
        return force;
    }

    /**
     * When false (the default), deleting an account that still has service tokens is rejected.
     * When true, the account is deleted and its token documents are left in place; recreating an
     * account with the same name re-enables any surviving tokens.
     */
    public void setForce(boolean force) {
        this.force = force;
    }

    public ServiceAccount.ServiceAccountId getAccountId() {
        return new ServiceAccount.ServiceAccountId(namespace, serviceName);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        final String principalError = ManagedServiceAccountIdValidator.validatePrincipal(getAccountId().asPrincipal());
        if (principalError != null) {
            validationException = addValidationError(principalError, validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(namespace);
        out.writeString(serviceName);
        refreshPolicy.writeTo(out);
        out.writeBoolean(force);
    }
}
