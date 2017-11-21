/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractProto;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.protocol.shared.Response;
import org.elasticsearch.xpack.sql.session.Cursor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.function.Function;

import static org.elasticsearch.rest.BytesRestResponse.TEXT_CONTENT_TYPE;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.xpack.sql.util.StringUtils.EMPTY;

public abstract class AbstractSqlProtocolRestAction extends BaseRestHandler {
    public static final NamedWriteableRegistry CURSOR_REGISTRY = new NamedWriteableRegistry(Cursor.getNamedWriteables());
    private final AbstractProto proto;

    protected AbstractSqlProtocolRestAction(Settings settings, AbstractProto proto) {
        super(settings);
        this.proto = proto;
    }

    protected abstract RestChannelConsumer innerPrepareRequest(Request request, Client client) throws IOException;

    protected <T> ActionListener<T> toActionListener(RestChannel channel, Function<T, Response> responseBuilder) {
        return new RestResponseListener<T>(channel) {
            @Override
            public RestResponse buildResponse(T response) throws Exception {
                try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
                    try (DataOutputStream dataOutputStream = new DataOutputStream(bytesStreamOutput)) {
                        // TODO use the version from the client
                        // Tracked by https://github.com/elastic/x-pack-elasticsearch/issues/3080
                        proto.writeResponse(responseBuilder.apply(response), Proto.CURRENT_VERSION, dataOutputStream);
                    }
                    return new BytesRestResponse(OK, TEXT_CONTENT_TYPE, bytesStreamOutput.bytes());
                }
            }
        };
    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        if (restRequest.method() == Method.HEAD) {
            return channel -> channel.sendResponse(new BytesRestResponse(OK, EMPTY));
        }
        Request request;
        try (DataInputStream in = new DataInputStream(restRequest.content().streamInput())) {
            request = proto.readRequest(in);
        }
        return innerPrepareRequest(request, client);
    }
}
