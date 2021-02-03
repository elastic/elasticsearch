/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestFlushAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_flush"),
            new Route(POST, "/_flush"),
            new Route(GET, "/{index}/_flush"),
            new Route(POST, "/{index}/_flush"));
    }

    @Override
    public String getName() {
        return "flush_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        FlushRequest flushRequest = new FlushRequest(Strings.splitStringByCommaToArray(request.param("index")));
        flushRequest.indicesOptions(IndicesOptions.fromRequest(request, flushRequest.indicesOptions()));
        flushRequest.force(request.paramAsBoolean("force", flushRequest.force()));
        flushRequest.waitIfOngoing(request.paramAsBoolean("wait_if_ongoing", flushRequest.waitIfOngoing()));
        return channel -> client.admin().indices().flush(flushRequest, new RestToXContentListener<>(channel));
    }
}
