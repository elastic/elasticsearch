/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.cli.net.protocol.QueryInitRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.QueryInitResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.QueryPageRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.QueryPageResponse;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlAction;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlRequest;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.protocol.shared.Response;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.joda.time.DateTimeZone;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.rest.BytesRestResponse.TEXT_CONTENT_TYPE;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestSqlCliAction extends BaseRestHandler {
    private final NamedWriteableRegistry cursorRegistry = new NamedWriteableRegistry(Cursor.getNamedWriteables());

    public RestSqlCliAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, "/_sql/cli", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        Request request;
        try (DataInputStream in = new DataInputStream(restRequest.content().streamInput())) {
            request = Proto.INSTANCE.readRequest(in);
        }
        Consumer<RestChannel> consumer = operation(cursorRegistry, request, client);
        return consumer::accept;
    }

    /**
     * Actual implementation of the operation
     */
    public static Consumer<RestChannel> operation(NamedWriteableRegistry cursorRegistry, Request request, Client client)
            throws IOException {
        RequestType requestType = (RequestType) request.requestType();
        switch (requestType) {
        case INFO:
            return channel -> client.execute(MainAction.INSTANCE, new MainRequest(), toActionListener(channel, response ->
                    new InfoResponse(response.getNodeName(), response.getClusterName().value(),
                            response.getVersion().major, response.getVersion().minor, response.getVersion().toString(),
                            response.getBuild().shortHash(), response.getBuild().date())));
        case QUERY_INIT:
            return queryInit(client, (QueryInitRequest) request);
        case QUERY_PAGE:
            return queryPage(cursorRegistry, client, (QueryPageRequest) request);
        default:
            throw new IllegalArgumentException("Unsupported action [" + requestType + "]");
        }
    }

    private static Consumer<RestChannel> queryInit(Client client, QueryInitRequest request) {
        // TODO time zone support for CLI
        SqlRequest sqlRequest = new SqlRequest(request.query, DateTimeZone.forTimeZone(request.timeZone), request.fetchSize, Cursor.EMPTY);
        long start = System.nanoTime();
        return channel -> client.execute(SqlAction.INSTANCE, sqlRequest, toActionListener(channel, response -> {
                CliFormatter formatter = new CliFormatter(response);
                String data = formatter.formatWithHeader(response);
                return new QueryInitResponse(System.nanoTime() - start, serializeCursor(response.cursor(), formatter), data);
        }));
    }

    private static Consumer<RestChannel> queryPage(NamedWriteableRegistry cursorRegistry, Client client, QueryPageRequest request) {
        Cursor cursor;
        CliFormatter formatter;
        try (StreamInput in = new NamedWriteableAwareStreamInput(new BytesArray(request.cursor).streamInput(), cursorRegistry)) {
            cursor = in.readNamedWriteable(Cursor.class);
            formatter = new CliFormatter(in);
        } catch (IOException e) {
            throw new IllegalArgumentException("error reading the cursor");
        }
        SqlRequest sqlRequest = new SqlRequest("", SqlRequest.DEFAULT_TIME_ZONE, -1, cursor);
        long start = System.nanoTime();
        return channel -> client.execute(SqlAction.INSTANCE, sqlRequest, toActionListener(channel, response -> {
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

    private static <T> ActionListener<T> toActionListener(RestChannel channel, Function<T, Response> responseBuilder) {
        // NOCOMMIT error response
        return new RestActionListener<T>(channel) {
            @Override
            protected void processResponse(T actionResponse) throws Exception {
                Response response = responseBuilder.apply(actionResponse);
                try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
                    try (DataOutputStream dataOutputStream = new DataOutputStream(bytesStreamOutput)) {
                        // NOCOMMIT use the version from the client
                        Proto.INSTANCE.writeResponse(response, Proto.CURRENT_VERSION, dataOutputStream);
                    }
                    channel.sendResponse(new BytesRestResponse(OK, TEXT_CONTENT_TYPE, bytesStreamOutput.bytes()));
                }
            }
        };
    }

    @Override
    public String getName() {
        return "xpack_sql_cli_action";
    }
}
