/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.jdbc.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.protocol.shared.Response;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

public class JdbcResponse extends ActionResponse {
    private BytesReference bytesReference;

    public JdbcResponse() {
    }

    public JdbcResponse(BytesReference bytesReference) {
        this.bytesReference = bytesReference;
    }

    public JdbcResponse(Response response) {
        try {
            response(response);
        } catch (IOException ex) {
            throw new IllegalArgumentException("cannot serialize the request", ex);
        }
    }

    /**
     * Gets the response object from internally stored serialized version
     *
     * @param request the request that was used to generate this response
     */
    public Response response(Request request) throws IOException {
        try (DataInputStream in = new DataInputStream(bytesReference.streamInput())) {
            return Proto.INSTANCE.readResponse(request, in);
        }
    }

    /**
     * Serialized the response object into internally stored serialized version
     */
    public JdbcResponse response(Response response) throws IOException {
        try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
            try (DataOutputStream dataOutputStream = new DataOutputStream(bytesStreamOutput)) {
                Proto.INSTANCE.writeResponse(response, Proto.CURRENT_VERSION, dataOutputStream);
            }
            bytesReference = bytesStreamOutput.bytes();
        }
        return this;
    }

    public BytesReference bytesReference() {
        return bytesReference;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JdbcResponse that = (JdbcResponse) o;
        return Objects.equals(bytesReference, that.bytesReference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bytesReference);
    }
}