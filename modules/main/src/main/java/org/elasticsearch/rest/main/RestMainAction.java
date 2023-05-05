/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.main;

import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.MainTransportRequest;
import org.elasticsearch.action.main.MainTransportResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

@ServerlessScope(Scope.PUBLIC)
public class RestMainAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/"), new Route(HEAD, "/"));
    }

    @Override
    public String getName() {
        return "main_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        return channel -> client.execute(
            MainAction.INSTANCE,
            new MainTransportRequest(),
            new RestBuilderListener<MainTransportResponse>(channel) {
                @Override
                public RestResponse buildResponse(MainTransportResponse mainResponse, XContentBuilder builder) throws Exception {
                    return convertMainResponse(mainResponse, request, builder);
                }
            }
        );
    }

    static RestResponse convertMainResponse(MainTransportResponse response, RestRequest request, XContentBuilder builder)
        throws IOException {
        // Default to pretty printing, but allow ?pretty=false to disable
        if (request.hasParam("pretty") == false) {
            builder.prettyPrint().lfAtEnd();
        }
        MainRestResponse mainRestResponse = new MainRestResponse(response);
        mainRestResponse.toXContent(builder, request);
        // response.toXContent(builder, request);
        return new RestResponse(RestStatus.OK, builder);
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
