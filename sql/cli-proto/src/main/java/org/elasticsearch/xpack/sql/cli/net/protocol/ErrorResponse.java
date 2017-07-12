/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.ResponseType;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractErrorResponse;
import org.elasticsearch.xpack.sql.protocol.shared.Request;

import java.io.DataInput;
import java.io.IOException;

/**
 * Response sent when there is a server side error.
 */
public class ErrorResponse extends AbstractErrorResponse<RequestType> {
    public ErrorResponse(RequestType requestType, String message, String cause, String stack) {
        super(requestType, message, cause, stack);
    }

    ErrorResponse(Request request, DataInput in) throws IOException {
        super((RequestType) request.requestType(), in);
    }

    @Override
    public ResponseType responseType() {
        return ResponseType.ERROR;
    }
}