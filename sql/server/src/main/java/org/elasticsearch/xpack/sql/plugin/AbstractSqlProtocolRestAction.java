/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.sql.analysis.AnalysisException;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto;
import org.elasticsearch.xpack.sql.parser.ParsingException;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractErrorResponse;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractExceptionResponse;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractProto;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractProto.SqlExceptionType;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.protocol.shared.Response;
import org.elasticsearch.xpack.sql.session.Cursor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.elasticsearch.rest.BytesRestResponse.TEXT_CONTENT_TYPE;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.xpack.sql.util.StringUtils.EMPTY;

public abstract class AbstractSqlProtocolRestAction extends BaseRestHandler {
    protected static final NamedWriteableRegistry CURSOR_REGISTRY = new NamedWriteableRegistry(Cursor.getNamedWriteables());
    private final AbstractProto proto;

    protected AbstractSqlProtocolRestAction(Settings settings, AbstractProto proto) {
        super(settings);
        this.proto = proto;
    }

    protected abstract RestChannelConsumer innerPrepareRequest(Request request, Client client) throws IOException;

    protected abstract AbstractExceptionResponse buildExceptionResponse(Request request, String message, String cause,
            SqlExceptionType exceptionType);

    protected abstract AbstractErrorResponse buildErrorResponse(Request request, String message, String cause, String stack);

    protected <T> ActionListener<T> toActionListener(Request request, RestChannel channel, Function<T, Response> responseBuilder) {
        return new ActionListener<T>() {
            @Override
            public void onResponse(T response) {
                try {
                    sendResponse(channel, responseBuilder.apply(response));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    sendResponse(channel, exceptionResponse(request, e));
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    logger.error("failed to send failure response", inner);
                }
            }
        };
    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        Request request;
        try (DataInputStream in = new DataInputStream(restRequest.content().streamInput())) {
            request = proto.readRequest(in);
        }
        try {
            return innerPrepareRequest(request, client);
        } catch (Exception e) {
            return channel -> sendResponse(channel, exceptionResponse(request, e));
        }
    }

    private Response exceptionResponse(Request request, Exception e) {
        // TODO I wonder why we don't just teach the servers to handle ES's normal exception response.....
        SqlExceptionType exceptionType = sqlExceptionType(e);

        String message = EMPTY;
        String cs = EMPTY;
        if (Strings.hasText(e.getMessage())) {
            message = e.getMessage();
        }
        cs = e.getClass().getName();

        if (exceptionType != null) {
            return buildExceptionResponse(request, message, cs, exceptionType);
        } else {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            return buildErrorResponse(request, message, cs, sw.toString());
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

    private void sendResponse(RestChannel channel, Response response) throws IOException {
        try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
            try (DataOutputStream dataOutputStream = new DataOutputStream(bytesStreamOutput)) {
                // NOCOMMIT use the version from the client
                proto.writeResponse(response, Proto.CURRENT_VERSION, dataOutputStream);
            }
            channel.sendResponse(new BytesRestResponse(OK, TEXT_CONTENT_TYPE, bytesStreamOutput.bytes()));
        }
    }
}
