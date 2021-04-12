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
import org.elasticsearch.common.ParseField;
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
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

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
    private static final ParseField GRID_PRECISION_FIELD = new ParseField("grid_precision");
    private static final ParseField GRID_TYPE_FIELD = new ParseField("grid_type");
    private static final ParseField EXTENT_FIELD = new ParseField("extent");
    private static final ParseField EXACT_BOUNDS_FIELD = new ParseField("exact_bounds");

    protected enum GRID_TYPE {
        GRID, POINT;

        static GRID_TYPE fromString(String type) {
            switch (type) {
                case "grid" : return GRID;
                case "point" : return POINT;
                default: throw new IllegalArgumentException("Invalid grid type [" + type + "]");
            }
        }
    }

    protected final ObjectParser<R, RestRequest> parser;

    @FunctionalInterface
    protected interface ResponseBuilder {
        void buildResponse(SearchResponse searchResponse, OutputStream outputStream) throws IOException;
    }

    private final Supplier<R> emptyRequestProvider;

    protected static class Request {
        private QueryBuilder queryBuilder;
        private String index;
        private String field;
        private int x;
        private int y;
        private int z;
        private Map<String, Object> runtimeMappings = emptyMap();
        private int gridPrecision = 8;
        private GRID_TYPE gridType = GRID_TYPE.GRID;
        private int size = 10000;
        private int extent = 4096;
        private AggregatorFactories.Builder aggBuilder;
        private List<FieldAndFormat> fields = emptyList();
        private boolean exact_bounds;

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

        public int getExtent() {
            return extent;
        }

        public void setExtent(int extent) {
            // TODO: validation
            this.extent = extent;
        }

        public boolean getExactBounds() {
            return exact_bounds;
        }

        public void setExactBounds(boolean exact_bounds) {
            this.exact_bounds = exact_bounds;
        }

        public List<FieldAndFormat> getFields() {
            return fields;
        }

        public void setFields(List<FieldAndFormat> fields) {
            this.fields = fields;
        }

        public QueryBuilder getQueryBuilder() {
            return queryBuilder;
        }

        public void setQueryBuilder(QueryBuilder queryBuilder) {
            // TODO: validation
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

        public Map<String, Object> getRuntimeMappings() {
            return runtimeMappings;
        }

        public void setRuntimeMappings(Map<String, Object> runtimeMappings) {
            this.runtimeMappings = runtimeMappings;
        }

        public int getGridPrecision() {
            return gridPrecision;
        }

        public void setGridPrecision(int gridPrecision) {
            if (gridPrecision < 0 || gridPrecision > 8) {
                throw new IllegalArgumentException("Invalid grid precision, value should be between 0 and 8, got [" + gridPrecision + "]");
            }
            this.gridPrecision = gridPrecision;
        }

        public GRID_TYPE getGridType() {
            return gridType;
        }

        public void setGridType(String gridType) {
            this.gridType = GRID_TYPE.fromString(gridType);
        }

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            // TODO: validation
            this.size = size;
        }

        public AggregatorFactories.Builder getAggBuilder() {
            return aggBuilder;
        }

        public void setAggBuilder(AggregatorFactories.Builder aggBuilder) {
            // TODO: validation
            this.aggBuilder = aggBuilder;
        }
    }

    protected AbstractVectorTileSearchAction(Supplier<R> emptyRequestProvider) {
        this.emptyRequestProvider = emptyRequestProvider;
        parser = new ObjectParser<>(getName(), emptyRequestProvider);
        parser.declareInt(Request::setSize, SearchSourceBuilder.SIZE_FIELD);
        parser.declareField(
            Request::setFields,
            AbstractVectorTileSearchAction::parseFetchFields,
            SearchSourceBuilder.FETCH_FIELDS_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY
        );
        parser.declareField(
            Request::setQueryBuilder,
            (CheckedFunction<XContentParser, QueryBuilder, IOException>) AbstractQueryBuilder::parseInnerQueryBuilder,
            SearchSourceBuilder.QUERY_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        parser.declareField(
            Request::setRuntimeMappings,
            XContentParser::map,
            SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        parser.declareField(
            Request::setAggBuilder,
            AggregatorFactories::parseAggregators,
            SearchSourceBuilder.AGGS_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        // Specific for vector tiles
        parser.declareInt(Request::setGridPrecision, GRID_PRECISION_FIELD);
        parser.declareInt(Request::setExtent, EXTENT_FIELD);
        parser.declareBoolean(Request::setExactBounds, EXACT_BOUNDS_FIELD);
        parser.declareString(Request::setGridType, GRID_TYPE_FIELD);
    }

    private static List<FieldAndFormat> parseFetchFields(XContentParser parser) throws IOException {
        List<FieldAndFormat> fetchFields = new ArrayList<>();
        while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            fetchFields.add(FieldAndFormat.fromXContent(parser));
        }
        return fetchFields;
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
