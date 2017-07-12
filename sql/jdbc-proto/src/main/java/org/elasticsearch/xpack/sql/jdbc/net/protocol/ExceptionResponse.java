/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.ResponseType;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractExceptionResponse;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractProto.SqlExceptionType;
import org.elasticsearch.xpack.sql.protocol.shared.Request;

import java.io.DataInput;
import java.io.IOException;

/**
 * Response sent when there is a client side error.
 */
public class ExceptionResponse extends AbstractExceptionResponse<RequestType> {
    public ExceptionResponse(RequestType requestType, String message, String cause, SqlExceptionType exceptionType) {
        super(requestType, message, cause, exceptionType);
    }

    ExceptionResponse(Request request, DataInput in) throws IOException {
        super((RequestType) request.requestType(), in);
    }

    @Override
    public ResponseType responseType() {
        return ResponseType.EXCEPTION;
    }
}
