/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search.persistent;

import org.elasticsearch.action.search.persistent.GetPersistentSearchAction;
import org.elasticsearch.action.search.persistent.GetPersistentSearchRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.search.persistent.PersistentSearchId;
import org.elasticsearch.search.persistent.PersistentSearchResponse;

import java.io.IOException;
import java.util.List;

public class RestGetPersistentSearchAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "get_persistent_search_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_persistent_search/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final PersistentSearchId persistentSearchId = PersistentSearchId.decode(request.param("id"));
        final GetPersistentSearchRequest getPersistentSearchRequest = new GetPersistentSearchRequest(persistentSearchId.getSearchId());
        return channel -> {
            RestCancellableNodeClient cancellableNodeClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancellableNodeClient.execute(GetPersistentSearchAction.INSTANCE, getPersistentSearchRequest,
                new RestResponseListener<>(channel) {
                    @Override
                    public RestResponse buildResponse(PersistentSearchResponse persistentSearchResponse) throws Exception {
                        if (persistentSearchResponse == null) {
                            return new BytesRestResponse(RestStatus.NOT_FOUND, request.param("id"));
                        } else {
                            final XContentBuilder builder = channel.newBuilder();
                            persistentSearchResponse.toXContent(builder, channel.request());
                            return new BytesRestResponse(RestStatus.OK, builder);
                        }
                    }
                });
        };
    }
}
