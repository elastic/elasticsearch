/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.fleet.action.PostSecretAction;
import org.elasticsearch.xpack.fleet.action.PostSecretRequest;
import org.elasticsearch.xpack.fleet.action.PostSecretResponse;

import java.io.IOException;
import java.util.List;

public class RestPostSecretsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "fleet_post_secrets";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, "/_fleet/secrets"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // TODO: does not throw IOException
        final String content = request.content().utf8ToString();
        return restChannel -> client.execute(
            PostSecretAction.INSTANCE,
            new PostSecretRequest(content, request.getXContentType()),
            new RestActionListener<>(restChannel) {
                @Override
                protected void processResponse(PostSecretResponse response) throws Exception {
                    // TODO: does not throw Exception
                    channel.sendResponse(new RestResponse(response.status(), XContentType.JSON.mediaType(), BytesArray.EMPTY));
                }
            }
        );
    }
}
