/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.cli.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto;
import org.elasticsearch.xpack.sql.protocol.shared.Request;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class CliRequest extends ActionRequest implements CompositeIndicesRequest {

    private BytesReference bytesReference;

    public CliRequest() {
    }

    public CliRequest(Request request) {
        try {
            request(request);
        } catch (IOException ex) {
            throw new IllegalArgumentException("cannot serialize the request", ex);
        }
    }

    public CliRequest(BytesReference bytesReference) {
        this.bytesReference = bytesReference;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (bytesReference == null) {
            validationException = addValidationError("no request has been specified", validationException);
        }
        return validationException;
    }

    /**
     * Gets the response object from internally stored serialized version
     */
    public Request request() throws IOException {
        try (DataInputStream in = new DataInputStream(bytesReference.streamInput())) {
            return Proto.INSTANCE.readRequest(in);
        }
    }

    /**
     * Converts the response object into internally stored serialized version
     */
    public CliRequest request(Request request) throws IOException {
        try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
            try (DataOutputStream dataOutputStream = new DataOutputStream(bytesStreamOutput)) {
                Proto.INSTANCE.writeRequest(request, dataOutputStream);
            }
            bytesReference = bytesStreamOutput.bytes();
        }
        return this;
    }

    public BytesReference bytesReference() {
        return bytesReference;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bytesReference);
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
        return Objects.equals(bytesReference, other.bytesReference);
    }

    @Override
    public String getDescription() {
        try {
            return "SQL CLI [" + request() + "]";
        } catch (IOException ex) {
            return "SQL CLI [" + ex.getMessage() + "]";
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        bytesReference = in.readBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBytesReference(bytesReference);
    }
}