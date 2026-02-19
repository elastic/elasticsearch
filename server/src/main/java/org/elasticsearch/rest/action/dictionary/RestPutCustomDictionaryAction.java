/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.dictionary;

import org.elasticsearch.action.dictionary.PutCustomDictionaryAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.PUBLIC)
public class RestPutCustomDictionaryAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "put_custom_dictionary_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_dictionary"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        restRequest.ensureContent();
        PutCustomDictionaryAction.Request request;
        try (XContentParser parser = restRequest.contentParser()) {
            request = PutCustomDictionaryAction.Request.PARSER.parse(parser, null);
        }

        return channel -> client.execute(PutCustomDictionaryAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
