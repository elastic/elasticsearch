/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.synonyms;

import org.elasticsearch.action.synonyms.PutSynonymsAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.PUBLIC)
public class RestPutSynonymsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "synonyms_put_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_synonyms/{synonymsSet}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        PutSynonymsAction.Request request = new PutSynonymsAction.Request(
            restRequest.param("synonymsSet"),
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
