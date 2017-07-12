/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.protocol.shared;

import org.elasticsearch.xpack.sql.protocol.shared.AbstractProto.RequestType;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractProto.SqlExceptionType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Response sent when there is a client side error.
 */
public abstract class AbstractExceptionResponse<RequestTypeT extends RequestType> extends Response {
    private final RequestTypeT requestType;
    public final String message, cause;
    private SqlExceptionType exceptionType;

    protected AbstractExceptionResponse(RequestTypeT requestType, String message, String cause, SqlExceptionType exceptionType) {
        if (requestType == null) {
            throw new IllegalArgumentException("[requestType] cannot be null");
        }
        if (message == null) {
            throw new IllegalArgumentException("[message] cannot be null");
        }
        if (cause == null) {
            throw new IllegalArgumentException("[cause] cannot be null");
        }
        if (exceptionType == null) {
            throw new IllegalArgumentException("[exceptionType] cannot be null");
        }
        this.requestType = requestType;
        this.message = message;
        this.cause = cause;
        this.exceptionType = exceptionType;
    }

    protected AbstractExceptionResponse(RequestTypeT requestType, DataInput in) throws IOException {
        this.requestType = requestType;
        message = in.readUTF();
        cause = in.readUTF();
        exceptionType = SqlExceptionType.read(in);
    }

    @Override
    protected final void write(int clientVersion, DataOutput out) throws IOException {
        out.writeUTF(message);
        out.writeUTF(cause);
        exceptionType.write(out);
    }

    @Override
    public RequestTypeT requestType() {
        return requestType;
    }

    @Override
    protected final String toStringBody() {
        return "request=[" + requestType
                + "] message=[" + message
                + "] cause=[" + cause
                + "] type=[" + exceptionType + "]";
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        AbstractExceptionResponse<?> other = (AbstractExceptionResponse<?>) obj;
        return Objects.equals(requestType, other.requestType)
                && Objects.equals(message, other.message)
                && Objects.equals(cause, other.cause)
                && Objects.equals(exceptionType, other.exceptionType);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(requestType, message, cause, exceptionType);
    }

    /**
     * Build an exception to throw for this failure. 
     */
    public SQLException asException() {
        return exceptionType.asException(message);
    }
}
