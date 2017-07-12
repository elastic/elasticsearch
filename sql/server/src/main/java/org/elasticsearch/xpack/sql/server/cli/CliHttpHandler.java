/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.server.cli;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractProto;
import org.elasticsearch.xpack.sql.server.AbstractSqlServer;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.io.DataInputStream;
import java.io.IOException;

import static org.elasticsearch.rest.BytesRestResponse.TEXT_CONTENT_TYPE;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.elasticsearch.rest.RestStatus.OK;

public class CliHttpHandler extends BaseRestHandler {

    public CliHttpHandler(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, "/_cli", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!request.hasContent()) {
            throw new IllegalArgumentException("expected a request body");
        }

        try (DataInputStream in = new DataInputStream(request.content().streamInput())) {
            CliRequest cliRequest = new CliRequest(Proto.INSTANCE.readRequest(in));
            return c -> client.executeLocally(CliAction.INSTANCE, cliRequest,
                                                ActionListener.wrap(response -> cliResponse(c, response), ex -> error(c, ex)));
        }
    }
    
    private static void cliResponse(RestChannel channel, CliResponse response) {
        BytesRestResponse restResponse = null;
        
        try {
            // NOCOMMIT use a real version
            restResponse = new BytesRestResponse(OK, TEXT_CONTENT_TYPE,
                    AbstractSqlServer.write(AbstractProto.CURRENT_VERSION, response.response()));
        } catch (IOException ex) {
            restResponse = new BytesRestResponse(INTERNAL_SERVER_ERROR, TEXT_CONTENT_TYPE, StringUtils.EMPTY);
        }

        channel.sendResponse(restResponse);
    }

    private static void error(RestChannel channel, Exception ex) {
        BytesRestResponse response = null;
        try {
            response = new BytesRestResponse(channel, ex);
        } catch (IOException e) {
            response = new BytesRestResponse(OK, TEXT_CONTENT_TYPE, ExceptionsHelper.stackTrace(e));
        }
        channel.sendResponse(response);
    }

    @Override
    public String getName() {
        return "sql_cli_action";
    }
}