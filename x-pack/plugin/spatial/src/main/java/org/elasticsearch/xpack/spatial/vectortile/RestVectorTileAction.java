/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.vectortile;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.xpack.core.spatial.action.VectorTileAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestVectorTileAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "{index}/_mvt/{field}/{z}/{x}/{y}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String index  = restRequest.param("index");
        String field  = restRequest.param("field");
        int z = Integer.parseInt(restRequest.param("z"));
        int x = Integer.parseInt(restRequest.param("x"));
        int y = Integer.parseInt(restRequest.param("y"));
        // TODO: Another possibility here is to create a search request with an specialize aggregation. The aggregation
        // will create the vector tile?
        VectorTileAction.Request request = new VectorTileAction.Request(index.split(","), field, z, x, y);
        return channel -> client.execute(VectorTileAction.INSTANCE, request, new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(VectorTileAction.Response response) throws Exception {
                BytesStream bytesOut = Streams.flushOnCloseStream(channel.bytesOutput());
                bytesOut.write(response.getVectorTiles());
                return new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, bytesOut.bytes());
            }
        });
    }

    @Override
    public String getName() {
        return "vectortile_action";
    }
}
