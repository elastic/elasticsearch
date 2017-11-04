/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.xpack.sql.cli.net.protocol.ErrorResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.ExceptionResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.cli.net.protocol.QueryInitRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.QueryInitResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.QueryPageRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.QueryPageResponse;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlAction;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlRequest;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractProto.SqlExceptionType;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.function.Consumer;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSqlCliAction extends AbstractSqlProtocolRestAction {
    public RestSqlCliAction(Settings settings, RestController controller) {
        super(settings, Proto.INSTANCE);
        controller.registerHandler(POST, "/_sql/cli", this);
    }

    @Override
    public String getName() {
        return "xpack_sql_cli_action";
        }

    @Override
    protected RestChannelConsumer innerPrepareRequest(Request request, Client client) throws IOException {
        Consumer<RestChannel> consumer = operation(request, client);
        return consumer::accept;
    }

    @Override
    protected ErrorResponse buildErrorResponse(Request request, String message, String cause, String stack) {
        return new ErrorResponse((RequestType) request.requestType(), message, cause, stack);
    }

    @Override
    protected ExceptionResponse buildExceptionResponse(Request request, String message, String cause, SqlExceptionType exceptionType) {
        return new ExceptionResponse((RequestType) request.requestType(), message, cause, exceptionType);
    }

    /**
     * Actual implementation of the operation
     */
    public Consumer<RestChannel> operation(Request request, Client client)
            throws IOException {
        RequestType requestType = (RequestType) request.requestType();
        switch (requestType) {
        case INFO:
            return channel -> client.execute(MainAction.INSTANCE, new MainRequest(), toActionListener(request, channel, response ->
                    new InfoResponse(response.getNodeName(), response.getClusterName().value(),
                            response.getVersion().major, response.getVersion().minor, response.getVersion().toString(),
                            response.getBuild().shortHash(), response.getBuild().date())));
        case QUERY_INIT:
            return queryInit(client, (QueryInitRequest) request);
        case QUERY_PAGE:
            return queryPage(client, (QueryPageRequest) request);
        default:
            throw new IllegalArgumentException("Unsupported action [" + requestType + "]");
        }
    }

    private Consumer<RestChannel> queryInit(Client client, QueryInitRequest request) {
        // TODO time zone support for CLI
        SqlRequest sqlRequest = new SqlRequest(request.query, DateTimeZone.forTimeZone(request.timeZone), request.fetchSize,
                                                TimeValue.timeValueMillis(request.timeout.requestTimeout),
                                                TimeValue.timeValueMillis(request.timeout.pageTimeout),
                                                Cursor.EMPTY);
        long start = System.nanoTime();
        return channel -> client.execute(SqlAction.INSTANCE, sqlRequest, toActionListener(request, channel, response -> {
            CliFormatter formatter = new CliFormatter(response);
            String data = formatter.formatWithHeader(response);
            return new QueryInitResponse(System.nanoTime() - start, serializeCursor(response.cursor(), formatter), data);
        }));
    }

    private Consumer<RestChannel> queryPage(Client client, QueryPageRequest request) {
        Cursor cursor;
        CliFormatter formatter;
        try (StreamInput in = new NamedWriteableAwareStreamInput(new BytesArray(request.cursor).streamInput(), CURSOR_REGISTRY)) {
            cursor = in.readNamedWriteable(Cursor.class);
            formatter = new CliFormatter(in);
        } catch (IOException e) {
            throw new IllegalArgumentException("error reading the cursor");
        }
        SqlRequest sqlRequest = new SqlRequest("", SqlRequest.DEFAULT_TIME_ZONE, 0,
                                                TimeValue.timeValueMillis(request.timeout.requestTimeout),
                                                TimeValue.timeValueMillis(request.timeout.pageTimeout),
                                                cursor);

        long start = System.nanoTime();
        return channel -> client.execute(SqlAction.INSTANCE, sqlRequest, toActionListener(request, channel, response -> {
            String data = formatter.formatWithoutHeader(response);
            return new QueryPageResponse(System.nanoTime() - start, serializeCursor(response.cursor(), formatter), data);
        }));
    }

    private static byte[] serializeCursor(Cursor cursor, CliFormatter formatter) {
        if (cursor == Cursor.EMPTY) {
            return new byte[0];
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeNamedWriteable(cursor);
            formatter.writeTo(out);
            return BytesRef.deepCopyOf(out.bytes().toBytesRef()).bytes;
        } catch (IOException e) {
            throw new RuntimeException("unexpected trouble building the cursor", e);
        }
    }
                    }
