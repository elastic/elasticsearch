/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.vectortile;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoGridBucket;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoTileGrid;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalGeoBounds;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestAggregatedVectorTileAction extends AbstractVectorTileSearchAction<RestAggregatedVectorTileAction.AggregatedRequest> {

    private static final String TYPE_PARAM = "type";
    private static final String GRID_TYPE = "grid";

    private static final String GRID_FIELD = "grid";
    private static final String BOUNDS_FIELD = "bounds";

    private static final ParseField SCALING = new ParseField("scaling");

    public RestAggregatedVectorTileAction() {
        super(AggregatedRequest::new);
        parser.declareField(
            AggregatedRequest::setRuntimeMappings,
            XContentParser::map,
            SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        parser.declareField(
            AggregatedRequest::setAggBuilder,
            AggregatorFactories::parseAggregators,
            SearchSourceBuilder.AGGS_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        parser.declareInt(AggregatedRequest::setScaling, SCALING);
    }

    protected static class AggregatedRequest extends AbstractVectorTileSearchAction.Request {
        private Map<String, Object> runtimeMappings = emptyMap();
        private int scaling = 8;
        private AggregatorFactories.Builder aggBuilder;

        public AggregatedRequest() {}

        public Map<String, Object> getRuntimeMappings() {
            return runtimeMappings;
        }

        public void setRuntimeMappings(Map<String, Object> runtimeMappings) {
            this.runtimeMappings = runtimeMappings;
        }

        public int getScaling() {
            return scaling;
        }

        public void setScaling(int scaling) {
            this.scaling = scaling;
        }

        public AggregatorFactories.Builder getAggBuilder() {
            return aggBuilder;
        }

        public void setAggBuilder(AggregatorFactories.Builder aggBuilder) {
            this.aggBuilder = aggBuilder;
        }
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "{index}/_agg_mvt/{field}/{z}/{x}/{y}"));
    }

    @Override
    protected ResponseBuilder doParseRequest(RestRequest restRequest, AggregatedRequest request, SearchRequestBuilder searchRequestBuilder)
        throws IOException {
        final boolean isGrid = restRequest.hasParam(TYPE_PARAM) && GRID_TYPE.equals(restRequest.param(TYPE_PARAM));

        searchBuilder(searchRequestBuilder, request);
        final int extent = 1 << request.getScaling();

        return (s, b) -> {
            // TODO: of there is no hits, should we return an empty tile with no layers or
            // a tile with empty layers?
            final VectorTile.Tile.Builder tileBuilder = VectorTile.Tile.newBuilder();
            final VectorTileGeometryBuilder geomBuilder = new VectorTileGeometryBuilder(
                request.getZ(),
                request.getX(),
                request.getY(),
                extent
            );
            final InternalGeoTileGrid grid = s.getAggregations().get(GRID_FIELD);
            tileBuilder.addLayers(getPointLayer(extent, isGrid, grid, geomBuilder));
            final InternalGeoBounds bounds = s.getAggregations().get(BOUNDS_FIELD);
            tileBuilder.addLayers(getMetaLayer(extent, bounds, geomBuilder));
            tileBuilder.build().writeTo(b);
        };
    }

    private VectorTile.Tile.Layer.Builder getPointLayer(
        int extent,
        boolean isGrid,
        InternalGeoTileGrid t,
        VectorTileGeometryBuilder geomBuilder
    ) {
        final VectorTile.Tile.Layer.Builder pointLayerBuilder = VectorTileUtils.createLayerBuilder("AGG", extent);
        pointLayerBuilder.addKeys("count");
        final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
        final VectorTile.Tile.Value.Builder valueBuilder = VectorTile.Tile.Value.newBuilder();
        final HashMap<Long, Integer> values = new HashMap<>();

        for (InternalGeoGridBucket<?> bucket : t.getBuckets()) {
            long count = bucket.getDocCount();
            if (count > 0) {
                featureBuilder.clear();
                // create geometry commands
                if (isGrid) {
                    Rectangle r = GeoTileUtils.toBoundingBox(bucket.getKeyAsString());
                    geomBuilder.box(featureBuilder, r.getMinLon(), r.getMaxLon(), r.getMinLat(), r.getMaxLat());
                } else {
                    GeoPoint point = (GeoPoint) bucket.getKey();
                    geomBuilder.point(featureBuilder, point.lon(), point.lat());
                }
                // Add count as key value pair
                featureBuilder.addTags(0);
                final int tagValue;
                if (values.containsKey(count)) {
                    tagValue = values.get(count);
                } else {
                    valueBuilder.clear();
                    valueBuilder.setIntValue(count);
                    tagValue = values.size();
                    pointLayerBuilder.addValues(valueBuilder);
                    values.put(count, tagValue);
                }
                featureBuilder.addTags(tagValue);
                pointLayerBuilder.addFeatures(featureBuilder);
            }
        }
        return pointLayerBuilder;
    }

    private VectorTile.Tile.Layer.Builder getMetaLayer(int extent, InternalGeoBounds t, VectorTileGeometryBuilder geomBuilder) {
        final VectorTile.Tile.Layer.Builder metaLayerBuilder = VectorTileUtils.createLayerBuilder("META", extent);
        final GeoPoint topLeft = t.topLeft();
        final GeoPoint bottomRight = t.bottomRight();
        if (topLeft != null && bottomRight != null) {
            final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
            geomBuilder.box(featureBuilder, topLeft.lon(), bottomRight.lon(), bottomRight.lat(), topLeft.lat());
            metaLayerBuilder.addFeatures(featureBuilder);
        }
        return metaLayerBuilder;
    }

    private static SearchRequestBuilder searchBuilder(SearchRequestBuilder searchRequestBuilder, AggregatedRequest request)
        throws IOException {
        Rectangle rectangle = request.getBoundingBox();
        int extent = 1 << request.getScaling();
        GeoBoundingBox boundingBox = new GeoBoundingBox(
            new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
            new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
        );
        GeoGridAggregationBuilder aBuilder = new GeoTileGridAggregationBuilder(GRID_FIELD).field(request.getField())
            .precision(Math.min(GeoTileUtils.MAX_ZOOM, request.getZ() + request.getScaling()))
            .setGeoBoundingBox(boundingBox)
            .size(extent * extent);
        if (request.getAggBuilder() != null) {
            aBuilder.subAggregations(request.getAggBuilder());
        }
        GeoBoundsAggregationBuilder boundsBuilder = new GeoBoundsAggregationBuilder(BOUNDS_FIELD).field(request.getField())
            .wrapLongitude(false);
        SearchRequestBuilder requestBuilder = searchRequestBuilder.addAggregation(aBuilder).addAggregation(boundsBuilder).setSize(0);
        if (request.getRuntimeMappings() != null) {
            requestBuilder.setRuntimeMappings(request.getRuntimeMappings());
        }
        return requestBuilder;
    }

    @Override
    public String getName() {
        return "vectortile_aggregation_action";
    }
}
