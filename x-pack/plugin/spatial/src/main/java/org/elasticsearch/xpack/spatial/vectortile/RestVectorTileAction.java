/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.vectortile;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.xpack.spatial.search.aggregations.InternalVectorTile;
import org.elasticsearch.xpack.spatial.search.aggregations.VectorTileAggregationBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestVectorTileAction extends AbstractVectorTileSearchAction {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "{index}/_mvt/{field}/{z}/{x}/{y}"));
    }

    @Override
    protected ResponseBuilder doParseRequest(
        RestRequest restRequest, String field, int z, int x, int y, SearchRequestBuilder searchRequestBuilder) throws IOException {
        QueryBuilder queryBuilder = null;
        if (restRequest.hasContent()) {
            try (XContentParser parser = restRequest.contentParser()) {
                XContentParser.Token token = parser.nextToken();
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new IllegalArgumentException("Invalid content format");
                }
                String currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                        if (currentFieldName.equals("query")) {
                            queryBuilder = AbstractQueryBuilder.parseInnerQueryBuilder(parser);
                        } else {
                            throw new IllegalArgumentException("Unsupported field " + currentFieldName);
                        }
                    } else {
                        throw new IllegalArgumentException("Invalid content format");
                    }
                }
            }
        }

        searchBuilder(searchRequestBuilder, field, z, x, y, queryBuilder);
        return (s, b) -> {
            InternalVectorTile t = s.getAggregations().get(field);
            // TODO: Error processing
            t.writeTileToStream(b);
        };
    }

    private static void searchBuilder(
        SearchRequestBuilder searchRequestBuilder,
        String field,
        int z,
        int x,
        int y,
        QueryBuilder queryBuilder
    ) throws IOException {
        final Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, z);
        QueryBuilder qBuilder = QueryBuilders.geoShapeQuery(field, rectangle);
        if (queryBuilder != null) {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            boolQueryBuilder.filter(queryBuilder);
            boolQueryBuilder.filter(qBuilder);
            qBuilder = boolQueryBuilder;
        }
        final VectorTileAggregationBuilder aBuilder = new VectorTileAggregationBuilder(field).field(field).z(z).x(x).y(y);
        searchRequestBuilder.setQuery(qBuilder).addAggregation(aBuilder).setSize(0);
    }

    @Override
    public String getName() {
        return "vectortile_action";
    }
}
