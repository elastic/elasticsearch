/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.ResponseType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Response sent when there is a server side error.
 */
public class ErrorResponse extends Response {
    private final RequestType requestType;
    public final String message, cause, stack;

    public ErrorResponse(RequestType requestType, String message, String cause, String stack) {
        this.requestType = requestType;
        this.message = message;
        this.cause = cause;
        this.stack = stack;
    }

    ErrorResponse(DataInput in) throws IOException {
        requestType = RequestType.read(in);
        message = in.readUTF();
        cause = in.readUTF();
        stack = in.readUTF();
    }

    @Override
    void write(int clientVersion, DataOutput out) throws IOException {
        requestType.write(out);
        out.writeUTF(message);
        out.writeUTF(cause);
        out.writeUTF(stack);
    }

    @Override
    protected String toStringBody() {
        return "request=[" + requestType
                + "] message=[" + message
                + "] cuase=[" + cause
                + "] stack=[" + stack + "]";
    }

    @Override
    RequestType requestType() {
        return requestType;
    }

    @Override
    ResponseType responseType() {
        return ResponseType.ERROR;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        ErrorResponse other = (ErrorResponse) obj;
        return Objects.equals(requestType, other.requestType)
                && Objects.equals(message, other.message)
                && Objects.equals(cause, other.cause)
                && Objects.equals(stack, other.stack);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestType, message, cause, stack);
    }

}
