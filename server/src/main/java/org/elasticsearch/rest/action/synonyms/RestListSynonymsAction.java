/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.synonyms;

import org.elasticsearch.action.synonyms.ListSynonymsAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.PUBLIC)
public class RestListSynonymsAction extends BaseRestHandler {

    private static final Integer DEFAULT_FROM_PARAM = 0;
    private static final Integer DEFAULT_SIZE_PARAM = 10;

    @Override
    public String getName() {
        return "synonyms_list_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_synonyms"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        ListSynonymsAction.Request request = new ListSynonymsAction.Request(
            restRequest.paramAsInt("from", DEFAULT_FROM_PARAM),
            restRequest.paramAsInt("size", DEFAULT_SIZE_PARAM)
        );
        return channel -> client.execute(ListSynonymsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
