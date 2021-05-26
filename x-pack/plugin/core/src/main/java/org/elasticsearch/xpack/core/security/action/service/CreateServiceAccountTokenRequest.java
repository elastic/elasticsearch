/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.support.Validation;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class CreateServiceAccountTokenRequest extends ActionRequest {

    private final String namespace;
    private final String serviceName;
    private final String tokenName;
    private WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.WAIT_UNTIL;

    public CreateServiceAccountTokenRequest(String namespace, String serviceName, String tokenName) {
        this.namespace = namespace;
        this.serviceName = serviceName;
        this.tokenName = tokenName;
    }

    public CreateServiceAccountTokenRequest(StreamInput in) throws IOException {
        super(in);
        this.namespace = in.readString();
        this.serviceName = in.readString();
        this.tokenName = in.readString();
        this.refreshPolicy = WriteRequest.RefreshPolicy.readFrom(in);
    }

    public String getNamespace() {
        return namespace;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getTokenName() {
        return tokenName;
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public void setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        this.refreshPolicy = Objects.requireNonNull(refreshPolicy, "refresh policy may not be null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        CreateServiceAccountTokenRequest that = (CreateServiceAccountTokenRequest) o;
        return Objects.equals(namespace, that.namespace) && Objects.equals(serviceName, that.serviceName)
            && Objects.equals(tokenName, that.tokenName) && refreshPolicy == that.refreshPolicy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, serviceName, tokenName, refreshPolicy);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(namespace);
        out.writeString(serviceName);
        out.writeString(tokenName);
        refreshPolicy.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isNullOrEmpty(namespace)) {
            validationException = addValidationError("service account namespace is required", validationException);
        }

        if (Strings.isNullOrEmpty(serviceName)) {
            validationException = addValidationError("service account service-name is required", validationException);
        }

        if (false == Validation.isValidServiceAccountTokenName(tokenName)) {
            validationException = addValidationError(Validation.formatInvalidServiceTokenNameErrorMessage(tokenName), validationException);
        }
        return validationException;
    }
}
