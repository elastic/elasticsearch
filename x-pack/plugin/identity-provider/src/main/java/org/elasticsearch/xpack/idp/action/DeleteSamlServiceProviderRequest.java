/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request object to remove a service provider (by Entity ID) from the IdP.
 */
public class DeleteSamlServiceProviderRequest extends ActionRequest {

    private final String entityId;
    private final WriteRequest.RefreshPolicy refreshPolicy;

    public DeleteSamlServiceProviderRequest(String entityId, WriteRequest.RefreshPolicy refreshPolicy) {
        this.entityId = entityId;
        this.refreshPolicy = Objects.requireNonNull(refreshPolicy, "Refresh policy may not be null");
    }

    public DeleteSamlServiceProviderRequest(StreamInput in) throws IOException {
        this.entityId = in.readString();
        this.refreshPolicy = WriteRequest.RefreshPolicy.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(entityId);
        refreshPolicy.writeTo(out);
    }

    public String getEntityId() {
        return entityId;
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isNullOrEmpty(entityId)) {
            validationException = addValidationError("The Service Provider Entity ID is required", validationException);
        }
        return validationException;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final DeleteSamlServiceProviderRequest that = (DeleteSamlServiceProviderRequest) o;
        return Objects.equals(entityId, that.entityId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityId, refreshPolicy);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + entityId + "," + refreshPolicy + "}";
    }
}
