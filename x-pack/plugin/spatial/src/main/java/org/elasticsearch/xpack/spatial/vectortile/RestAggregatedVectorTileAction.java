/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.vectortile;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoGridBucket;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoTileGrid;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalGeoBounds;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestAggregatedVectorTileAction extends AbstractVectorTileSearchAction {

    private static final String TYPE_PARAM = "type";
    private static final String GRID_TYPE = "grid";

    private static final String GRID_FIELD = "grid";
    private static final String BOUNDS_FIELD = "bounds";

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "{index}/_agg_mvt/{field}/{z}/{x}/{y}"));
    }

    @Override
    protected ResponseBuilder doParseRequest(
        RestRequest restRequest, String field, int z, int x, int y, SearchRequestBuilder searchRequestBuilder) throws IOException {
        final boolean isGrid = restRequest.hasParam(TYPE_PARAM) && GRID_TYPE.equals(restRequest.param(TYPE_PARAM));

        final VectorTileAggConfig config = resolveConfig(restRequest);
        searchBuilder(searchRequestBuilder, field, z, x, y,  config);
        final int extent = 1 << config.getScaling();

        return (s, b) -> {
            // TODO: of there is no hits, should we return an empty tile with no layers or
            // a tile with empty layers?
            final VectorTile.Tile.Builder tileBuilder = VectorTile.Tile.newBuilder();
            final VectorTileGeometryBuilder geomBuilder = new VectorTileGeometryBuilder(z, x, y, extent);
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

    private static SearchRequestBuilder searchBuilder(
        SearchRequestBuilder searchRequestBuilder,
        String field,
        int z,
        int x,
        int y,
        VectorTileAggConfig config
    ) throws IOException {
        Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, z);
        QueryBuilder qBuilder = QueryBuilders.geoShapeQuery(field, rectangle);
        if (config.getQueryBuilder() != null) {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            boolQueryBuilder.filter(config.getQueryBuilder());
            boolQueryBuilder.filter(qBuilder);
            qBuilder = boolQueryBuilder;
        }
        int extent = 1 << config.getScaling();
        GeoBoundingBox boundingBox = new GeoBoundingBox(
            new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
            new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
        );
        GeoGridAggregationBuilder aBuilder = new GeoTileGridAggregationBuilder(GRID_FIELD).field(field)
            .precision(Math.min(GeoTileUtils.MAX_ZOOM, z + config.getScaling()))
            .setGeoBoundingBox(boundingBox)
            .size(extent * extent);
        if (config.getAggBuilder() != null) {
            aBuilder.subAggregations(config.getAggBuilder());
        }
        GeoBoundsAggregationBuilder boundsBuilder = new GeoBoundsAggregationBuilder(BOUNDS_FIELD).field(field).wrapLongitude(false);
        SearchRequestBuilder requestBuilder = searchRequestBuilder.setQuery(qBuilder)
            .addAggregation(aBuilder)
            .addAggregation(boundsBuilder)
            .setSize(0);
        if (config.getRuntimeMappings() != null) {
            requestBuilder.setRuntimeMappings(config.getRuntimeMappings());
        }
        return requestBuilder;
    }

    private VectorTileAggConfig resolveConfig(RestRequest restRequest) throws IOException {
        if (restRequest.hasContent()) {
            try (XContentParser parser = restRequest.contentParser()) {
                return VectorTileAggConfig.PARSER.apply(parser, null);
            }
        } else {
            return VectorTileAggConfig.getInstance();
        }
    }

    @Override
    public String getName() {
        return "vectortile_aggregation_action";
    }
}
