/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.vectortile;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Supplier;

/**
 * Base class for rest actions that performs a search and translates it into
 * a protobuf response
 */
public abstract class AbstractVectorTileSearchAction<R extends AbstractVectorTileSearchAction.Request> extends BaseRestHandler {

    private static final String INDEX_PARAM = "index";
    private static final String FIELD_PARAM = "field";
    private static final String Z_PARAM = "z";
    private static final String X_PARAM = "x";
    private static final String Y_PARAM = "y";

    protected final ObjectParser<R, RestRequest> parser;

    @FunctionalInterface
    protected interface ResponseBuilder {
        void buildResponse(SearchResponse searchResponse, OutputStream outputStream) throws IOException;
    }

    private final Supplier<R> emptyRequestProvider;

    protected static class Request {
        QueryBuilder queryBuilder;
        String index;
        String field;
        int x;
        int y;
        int z;

        public String getIndex() {
            return index;
        }

        public void setIndex(String index) {
            this.index = index;
        }

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        public int getX() {
            return x;
        }

        public void setX(int x) {
            this.x = x;
        }

        public int getY() {
            return y;
        }

        public void setY(int y) {
            this.y = y;
        }

        public int getZ() {
            return z;
        }

        public void setZ(int z) {
            this.z = z;
        }

        public QueryBuilder getQueryBuilder() {
            return queryBuilder;
        }

        public void setQueryBuilder(QueryBuilder queryBuilder) {
            this.queryBuilder = queryBuilder;
        }

        public Rectangle getBoundingBox() {
            return GeoTileUtils.toBoundingBox(x, y, z);
        }

        public QueryBuilder getQuery() throws IOException {
            QueryBuilder qBuilder = QueryBuilders.geoShapeQuery(field, getBoundingBox());
            if (queryBuilder != null) {
                BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                boolQueryBuilder.filter(queryBuilder);
                boolQueryBuilder.filter(qBuilder);
                qBuilder = boolQueryBuilder;
            }
            return qBuilder;
        }
    }

    protected AbstractVectorTileSearchAction(Supplier<R> emptyRequestProvider) {
        this.emptyRequestProvider = emptyRequestProvider;
        parser = new ObjectParser<>(getName(), emptyRequestProvider);
        parser.declareField(
            Request::setQueryBuilder,
            (CheckedFunction<XContentParser, QueryBuilder, IOException>) AbstractQueryBuilder::parseInnerQueryBuilder,
            SearchSourceBuilder.QUERY_FIELD,
            ObjectParser.ValueType.OBJECT
        );
    }

    protected abstract ResponseBuilder doParseRequest(RestRequest restRequest, R request, SearchRequestBuilder searchRequestBuilder)
        throws IOException;

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        final R request;
        if (restRequest.hasContent()) {
            try (XContentParser contentParser = restRequest.contentParser()) {
                request = parser.parse(contentParser, restRequest);
            }
        } else {
            request = emptyRequestProvider.get();
        }
        request.setIndex(restRequest.param(INDEX_PARAM));
        request.setField(restRequest.param(FIELD_PARAM));
        request.setZ(Integer.parseInt(restRequest.param(Z_PARAM)));
        request.setX(Integer.parseInt(restRequest.param(X_PARAM)));
        request.setY(Integer.parseInt(restRequest.param(Y_PARAM)));

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(Strings.splitStringByCommaToArray(request.getIndex()));
        searchRequestBuilder.setQuery(request.getQuery());
        searchRequestBuilder.setSize(0);
        ResponseBuilder responseBuilder = doParseRequest(restRequest, request, searchRequestBuilder);

        // TODO: how do we handle cancellations?
        return channel -> searchRequestBuilder.execute(new RestResponseListener<>(channel) {

            @Override
            public RestResponse buildResponse(SearchResponse searchResponse) throws Exception {
                try (BytesStream bytesOut = Streams.flushOnCloseStream(channel.bytesOutput())) {
                    responseBuilder.buildResponse(searchResponse, bytesOut);
                    return new BytesRestResponse(RestStatus.OK, "application/x-protobuf", bytesOut.bytes());
                }
            }
        });
    }

}
