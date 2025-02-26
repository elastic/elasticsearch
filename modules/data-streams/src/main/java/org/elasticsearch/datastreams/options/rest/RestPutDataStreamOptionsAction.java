/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.options.rest;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.datastreams.options.action.PutDataStreamOptionsAction;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.PUBLIC)
public class RestPutDataStreamOptionsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "put_data_stream_options_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_data_stream/{name}/_options"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            PutDataStreamOptionsAction.Request putOptionsRequest = PutDataStreamOptionsAction.Request.parseRequest(
                parser,
                (failureStore) -> new PutDataStreamOptionsAction.Request(
                    getMasterNodeTimeout(request),
                    getAckTimeout(request),
                    Strings.splitStringByCommaToArray(request.param("name")),
                    failureStore
                )
            );
            putOptionsRequest.indicesOptions(IndicesOptions.fromRequest(request, putOptionsRequest.indicesOptions()));
            return channel -> client.execute(PutDataStreamOptionsAction.INSTANCE, putOptionsRequest, new RestToXContentListener<>(channel));
        }
    }
}
