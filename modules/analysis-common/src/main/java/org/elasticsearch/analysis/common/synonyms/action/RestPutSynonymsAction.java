/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common.synonyms.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutSynonymsAction extends BaseRestHandler {

    // TODO Move to plugin or base class
    private static final String SYNONYMS_API_ENDPOINT = "_synonyms";

    @Override
    public String getName() {
        return "synonyms_put_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/" + SYNONYMS_API_ENDPOINT + "/{name}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        PutSynonymsAction.Request request = new PutSynonymsAction.Request(
            restRequest.param("name"),
            restRequest.content(),
            restRequest.getXContentType()
        );
        return channel -> client.execute(PutSynonymsAction.INSTANCE, request, new RestToXContentListener<>(channel) {
            @Override
            protected RestStatus getStatus(PutSynonymsAction.Response response) {
                return response.status();
            }
        });
    }
}
