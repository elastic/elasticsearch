/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.secrets.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.application.connector.secrets.ConnectorSecretsConstants;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DeleteConnectorSecretRequest extends LegacyActionRequest {

    private final String id;

    public DeleteConnectorSecretRequest(String id) {
        this.id = id;
    }

    public DeleteConnectorSecretRequest(StreamInput in) throws IOException {
        super(in);
        this.id = in.readString();
    }

    public String id() {
        return id;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        if (Strings.isNullOrEmpty(id)) {
            validationException = addValidationError(
                ConnectorSecretsConstants.CONNECTOR_SECRET_ID_NULL_OR_EMPTY_MESSAGE,
                validationException
            );
        }

        return validationException;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeleteConnectorSecretRequest that = (DeleteConnectorSecretRequest) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
