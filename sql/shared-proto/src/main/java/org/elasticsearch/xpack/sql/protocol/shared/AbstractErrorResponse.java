/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.protocol.shared;

import org.elasticsearch.xpack.sql.protocol.shared.AbstractProto.RequestType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Response sent when there is a server side error.
 */
public abstract class AbstractErrorResponse<RequestTypeT extends RequestType> extends Response {
    private final RequestTypeT requestType;
    public final String message, cause, stack;

    protected AbstractErrorResponse(RequestTypeT requestType, String message, String cause, String stack) {
        this.requestType = requestType;
        this.message = message;
        this.cause = cause;
        this.stack = stack;
    }

    protected AbstractErrorResponse(RequestTypeT requestType, DataInput in) throws IOException {
        this.requestType = requestType;
        message = in.readUTF();
        cause = in.readUTF();
        stack = in.readUTF();
    }

    @Override
    protected final void write(int clientVersion, DataOutput out) throws IOException {
        out.writeUTF(message);
        out.writeUTF(cause);
        out.writeUTF(stack);
    }

    @Override
    public RequestTypeT requestType() { // NOCOMMIT do I need the T?
        return requestType;
    }

    @Override
    protected final String toStringBody() {
        return "request=[" + requestType
                + "] message=[" + message
                + "] cause=[" + cause
                + "] stack=[" + stack + "]";
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        AbstractErrorResponse<?> other = (AbstractErrorResponse<?>) obj;
        return Objects.equals(requestType, other.requestType)
                && Objects.equals(message, other.message)
                && Objects.equals(cause, other.cause)
                && Objects.equals(stack, other.stack);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(message, cause, stack);
    }
}
