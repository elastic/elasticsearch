/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DeleteInternalCcrRepositoryRequest extends ActionRequest {

    private String name;

    DeleteInternalCcrRepositoryRequest() {
    }

    public DeleteInternalCcrRepositoryRequest(String name) {
        this.name = Objects.requireNonNull(name);
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (name == null) {
            validationException = addValidationError("name is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("DeleteInternalRepositoryRequest cannot be serialized for sending across the wire.");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("DeleteInternalRepositoryRequest cannot be serialized for sending across the wire.");
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeleteInternalCcrRepositoryRequest that = (DeleteInternalCcrRepositoryRequest) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "DeleteInternalRepositoryRequest{" +
            "name='" + name + '\'' +
            '}';
    }
}
