/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.vectortile;


import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.xpack.spatial.search.aggregations.InternalVectorTile;
import org.elasticsearch.xpack.spatial.search.aggregations.VectorTileAggregationBuilder;

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
        final String index  = restRequest.param("index");
        final String field  = restRequest.param("field");
        final int z = Integer.parseInt(restRequest.param("z"));
        final int x = Integer.parseInt(restRequest.param("x"));
        final int y = Integer.parseInt(restRequest.param("y"));
        final SearchRequestBuilder builder = searchBuilder(client, index.split(","), field, z, x, y);
        // TODO: how do we handle cancellations?
        return channel -> builder.execute( new RestResponseListener<>(channel) {

            @Override
            public RestResponse buildResponse(SearchResponse searchResponse) throws Exception {
                final InternalVectorTile t = searchResponse.getAggregations().get(field);
                final BytesStream bytesOut = Streams.flushOnCloseStream(channel.bytesOutput());
                bytesOut.write(t.getVectorTile());
                return new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, bytesOut.bytes());
            }
        });
    }

    public static SearchRequestBuilder searchBuilder(Client client, String[] index, String field, int z, int x, int y) throws IOException {
        final Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, z);
        final GeoShapeQueryBuilder qBuilder = QueryBuilders.geoShapeQuery(field, rectangle);
        final VectorTileAggregationBuilder aBuilder = new VectorTileAggregationBuilder(field).field(field).z(z).x(x).y(y);
        return client.prepareSearch(index).setQuery(qBuilder).addAggregation(aBuilder).setSize(0);
    }

    @Override
    public String getName() {
        return "vectortile_action";
    }
}
