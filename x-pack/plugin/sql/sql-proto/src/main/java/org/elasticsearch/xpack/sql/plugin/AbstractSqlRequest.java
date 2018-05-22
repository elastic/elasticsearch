/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.xpack.sql.proto.Mode;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Base request for all SQL-related requests.
 * <p>
 * Contains information about the client mode that can be used to generate different responses based on the caller type.
 */
public abstract class AbstractSqlRequest extends ActionRequest implements ToXContent {

    private Mode mode = Mode.PLAIN;

    protected AbstractSqlRequest() {

    }

    protected AbstractSqlRequest(Mode mode) {
        this.mode = mode;
    }

    protected AbstractSqlRequest(StreamInput in) throws IOException {
        super(in);
        mode = in.readEnum(Mode.class);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (mode == null) {
            validationException = addValidationError("[mode] is required", validationException);
        }
        return validationException;
    }

    @Override
    public final void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(mode);
    }

    public Mode mode() {
        return mode;
    }

    public void mode(Mode mode) {
        this.mode = mode;
    }

    public void mode(String mode) {
        this.mode = Mode.fromString(mode);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractSqlRequest that = (AbstractSqlRequest) o;
        return mode == that.mode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(mode);
    }

}
