/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.server;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.xpack.sql.analysis.AnalysisException;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto;
import org.elasticsearch.xpack.sql.parser.ParsingException;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractErrorResponse;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractExceptionResponse;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractProto.SqlExceptionType;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.protocol.shared.Response;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.xpack.sql.util.StringUtils.EMPTY;

public abstract class AbstractSqlServer {
    public final void handle(Request req, ActionListener<Response> listener) {
        try {
            innerHandle(req, listener);
        } catch (Exception e) {
            listener.onResponse(exceptionResponse(req, e));
        }
    }

    protected final Response exceptionResponse(Request req, Exception e) {
        // NOCOMMIT I wonder why we don't just teach the servers to handle ES's normal exception response.....
        SqlExceptionType exceptionType = sqlExceptionType(e);

        String message = EMPTY;
        String cs = EMPTY;
        if (Strings.hasText(e.getMessage())) {
            message = e.getMessage();
        }
        cs = e.getClass().getName();

        if (exceptionType != null) {
            return buildExceptionResponse(req, message, cs, exceptionType);
        } else {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            return buildErrorResponse(req, message, cs, sw.toString());
        }
    }

    protected abstract void innerHandle(Request req, ActionListener<Response> listener);
    protected abstract AbstractExceptionResponse<?> buildExceptionResponse(Request request, String message, String cause,
            SqlExceptionType exceptionType);
    protected abstract AbstractErrorResponse<?> buildErrorResponse(Request request, String message, String cause, String stack);

    public static BytesReference write(int clientVersion, Response response) throws IOException {
        try (BytesStreamOutput array = new BytesStreamOutput();
            DataOutputStream out = new DataOutputStream(array)) {
            Proto.INSTANCE.writeResponse(response, clientVersion, out);
            out.flush();
            return array.bytes();
        }
    }

    private static SqlExceptionType sqlExceptionType(Throwable cause) {
        if (cause instanceof AnalysisException || cause instanceof ResourceNotFoundException) {
            return SqlExceptionType.DATA;
        }
        if (cause instanceof ParsingException) {
            return SqlExceptionType.SYNTAX;
        }
        if (cause instanceof TimeoutException) {
            return SqlExceptionType.TIMEOUT;
        }

        return null;
    }
}
