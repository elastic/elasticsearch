/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetServiceAccountCredentialsRequest extends ActionRequest {

    private final String namespace;
    private final String serviceName;

    public GetServiceAccountCredentialsRequest(String namespace, String serviceName) {
        this.namespace = namespace;
        this.serviceName = serviceName;
    }

    public GetServiceAccountCredentialsRequest(StreamInput in) throws IOException {
        super(in);
        this.namespace = in.readString();
        this.serviceName = in.readString();
    }

    public String getNamespace() {
        return namespace;
    }

    public String getServiceName() {
        return serviceName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        GetServiceAccountCredentialsRequest that = (GetServiceAccountCredentialsRequest) o;
        return Objects.equals(namespace, that.namespace) && Objects.equals(serviceName, that.serviceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, serviceName);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(namespace);
        out.writeString(serviceName);
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
        return validationException;
    }
}
