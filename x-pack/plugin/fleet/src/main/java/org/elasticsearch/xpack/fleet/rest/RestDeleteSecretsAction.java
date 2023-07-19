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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.fleet.action.DeleteSecretAction;
import org.elasticsearch.xpack.fleet.action.DeleteSecretRequest;
import org.elasticsearch.xpack.fleet.action.DeleteSecretResponse;

import java.io.IOException;
import java.util.List;

public class RestDeleteSecretsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "fleet_delete_secrets";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.DELETE, "/_fleet/_fleet_secrets/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // TODO: does not throw IOException
        final String id = request.param("id");
        return restChannel -> client.execute(
            DeleteSecretAction.INSTANCE,
            new DeleteSecretRequest(id),
            new RestActionListener<>(restChannel) {
                @Override
                protected void processResponse(DeleteSecretResponse response) throws Exception {
                    // TODO: does not throw Exception
                    final RestStatus status = response.isDeleted() ? RestStatus.OK : RestStatus.NOT_FOUND;
                    channel.sendResponse(new RestResponse(status, XContentType.JSON.mediaType(), BytesArray.EMPTY));
                }
            }
        );
    }
}
