/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.vectortile;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.spatial.search.aggregations.InternalVectorTile;
import org.elasticsearch.xpack.spatial.search.aggregations.VectorTileAggregationBuilder;
import org.elasticsearch.xpack.spatial.vectortile.AbstractVectorTileSearchAction.Request;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestVectorTileAction extends AbstractVectorTileSearchAction<Request> {

    public RestVectorTileAction() {
        super(Request::new);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "{index}/_mvt/{field}/{z}/{x}/{y}"));
    }

    @Override
    protected ResponseBuilder doParseRequest(RestRequest restRequest, Request request, SearchRequestBuilder searchRequestBuilder) {
        final VectorTileAggregationBuilder aBuilder = new VectorTileAggregationBuilder(request.getField())
            .field(request.getField())
            .z(request.getZ())
            .x(request.getX())
            .y(request.getY());
        searchRequestBuilder.addAggregation(aBuilder).setSize(0);
        return (s, b) -> {
            InternalVectorTile t = s.getAggregations().get(request.getField());
            // TODO: Error processing
            t.writeTileToStream(b);
        };
    }

    @Override
    public String getName() {
        return "vectortile_action";
    }
}
