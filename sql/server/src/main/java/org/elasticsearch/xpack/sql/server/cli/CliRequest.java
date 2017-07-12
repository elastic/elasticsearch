/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.server.cli;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.xpack.sql.protocol.shared.Request;

import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class CliRequest extends ActionRequest implements CompositeIndicesRequest {

    private Request request;

    public CliRequest() {}

    public CliRequest(Request request) {
        this.request = request;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (request == null) {
            validationException = addValidationError("no request has been specified", validationException);
        }
        return validationException;
    }

    public Request request() {
        return request;
    }

    public CliRequest request(Request request) {
        this.request = request;
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(request);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        CliRequest other = (CliRequest) obj;
        return Objects.equals(request, other.request);
    }

    @Override
    public String getDescription() {
        return "SQL CLI [" + request + "]";
    }
}