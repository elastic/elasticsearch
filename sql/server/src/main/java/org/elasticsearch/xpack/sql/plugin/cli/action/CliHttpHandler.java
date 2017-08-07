/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.cli.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;

import static org.elasticsearch.rest.BytesRestResponse.TEXT_CONTENT_TYPE;
import static org.elasticsearch.rest.RestRequest.Method.POST;
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

        CliRequest cliRequest = new CliRequest(request.content());
        return c -> client.executeLocally(CliAction.INSTANCE, cliRequest,
                ActionListener.wrap(response -> c.sendResponse(new BytesRestResponse(OK, TEXT_CONTENT_TYPE, response.bytesReference())),
                        ex -> error(c, ex)));
    }

    private static void error(RestChannel channel, Exception ex) {
        BytesRestResponse response;
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