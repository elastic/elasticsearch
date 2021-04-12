/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.vectortile;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.adapt.jts.IUserDataConverter;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerProps;
import com.wdtinc.mapbox_vector_tile.encoding.MvtValue;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoGridBucket;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoTileGrid;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalGeoBounds;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestVectorTileAction extends AbstractVectorTileSearchAction<AbstractVectorTileSearchAction.Request> {

    private static final String META_LAYER = "meta";
    private static final String HITS_LAYER = "hits";
    private static final String AGGS_LAYER = "aggs";

    private static final String GRID_FIELD = "grid";
    private static final String BOUNDS_FIELD = "bounds";

    private static final String COUNT_TAG = "count";
    private static final String ID_TAG = "id";

    public RestVectorTileAction() {
        super(Request::new);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "{index}/_mvt/{field}/{z}/{x}/{y}"));
    }

    @Override
    protected ResponseBuilder doParseRequest(RestRequest restRequest, Request request, SearchRequestBuilder searchRequestBuilder) {
        final int extent = request.getExtent();
        searchBuilder(searchRequestBuilder, request);
        return (s, b) -> {
            // Even if there is no hits, we return a tile with the meta layer
            final VectorTile.Tile.Builder tileBuilder = VectorTile.Tile.newBuilder();
            final VectorTileGeometryBuilder geomBuilder = new VectorTileGeometryBuilder(
                request.getZ(),
                request.getX(),
                request.getY(),
                extent
            );
            final SearchHit[] hits = s.getHits().getHits();
            if (hits.length > 0) {
                tileBuilder.addLayers(getHitsLayer(s, request));
            }
            final InternalGeoTileGrid grid = s.getAggregations() != null ? s.getAggregations().get(GRID_FIELD) : null;
            // TODO: should be expose the total number of buckets on InternalGeoTileGrid?
            if (grid != null && grid.getBuckets().size() > 0) {
                tileBuilder.addLayers(getAggsLayer(s, request, geomBuilder));
            }
            tileBuilder.addLayers(getMetaLayer(s, request, geomBuilder));
            tileBuilder.build().writeTo(b);
        };
    }

    private static SearchRequestBuilder searchBuilder(SearchRequestBuilder searchRequestBuilder, Request request) {
        searchRequestBuilder.setSize(request.getSize());
        searchRequestBuilder.setFetchSource(false);
        // TODO: I wonder if we can leverage field and format so what we get in the result is already the mvt commands.
        searchRequestBuilder.addFetchField(new FieldAndFormat(request.getField(), null));
        for (FieldAndFormat field : request.getFields()) {
            searchRequestBuilder.addFetchField(field);
        }
        searchRequestBuilder.setRuntimeMappings(request.getRuntimeMappings());
        if (request.getGridPrecision() > 0) {
            final Rectangle rectangle = request.getBoundingBox();
            final GeoBoundingBox boundingBox = new GeoBoundingBox(
                new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
                new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
            );
            final int extent = 1 << request.getGridPrecision();
            final GeoGridAggregationBuilder aBuilder = new GeoTileGridAggregationBuilder(GRID_FIELD).field(request.getField())
                .precision(Math.min(GeoTileUtils.MAX_ZOOM, request.getZ() + request.getGridPrecision()))
                .setGeoBoundingBox(boundingBox)
                .size(extent * extent);
            if (request.getAggBuilder() != null) {
                aBuilder.subAggregations(request.getAggBuilder());
            }
            searchRequestBuilder.addAggregation(aBuilder);
        }
        if (request.getExactBounds()) {
            final GeoBoundsAggregationBuilder boundsBuilder =
                new GeoBoundsAggregationBuilder(BOUNDS_FIELD).field(request.getField()).wrapLongitude(false);
            searchRequestBuilder.addAggregation(boundsBuilder);
        }
        return searchRequestBuilder;
    }

    private VectorTile.Tile.Layer.Builder getHitsLayer(SearchResponse response, Request request) {
        final FeatureFactory featureFactory = new FeatureFactory(request.getZ(), request.getX(), request.getY(), request.getExtent());
        final GeometryParser parser = new GeometryParser(true, false, false);
        final VectorTile.Tile.Layer.Builder hitsLayerBuilder = VectorTileUtils.createLayerBuilder(HITS_LAYER, request.getExtent());
        final List<FieldAndFormat> fields = request.getFields();
        for (SearchHit searchHit : response.getHits()) {
            final IUserDataConverter tags = (userData, layerProps, featureBuilder) -> {
                // TODO: It would be great if we can add the centroid information for polygons. That information can be
                // used to place labels inside those geometries
                addPropertyToFeature(featureBuilder, layerProps, ID_TAG, searchHit.getId());
                if (fields != null) {
                    for (FieldAndFormat field : fields) {
                        DocumentField documentField = searchHit.field(field.field);
                        if (documentField != null) {
                            addPropertyToFeature(featureBuilder, layerProps, field.field, documentField.getValue());
                        }
                    }
                }
            };
            // TODO: See comment on field formats.
            final Geometry geometry = parser.parseGeometry(searchHit.field(request.getField()).getValue());
            hitsLayerBuilder.addAllFeatures(featureFactory.getFeatures(geometry, tags));
        }
        addPropertiesToLayer(hitsLayerBuilder, featureFactory.getLayerProps());
        return hitsLayerBuilder;
    }

    private VectorTile.Tile.Layer.Builder getAggsLayer(SearchResponse response, Request request, VectorTileGeometryBuilder geomBuilder) {
        final VectorTile.Tile.Layer.Builder aggLayerBuilder = VectorTileUtils.createLayerBuilder(AGGS_LAYER, request.getExtent());
        final MvtLayerProps layerProps = new MvtLayerProps();
        final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
        final InternalGeoTileGrid grid = response.getAggregations().get(GRID_FIELD);
        for (InternalGeoGridBucket<?> bucket : grid.getBuckets()) {
            final long count = bucket.getDocCount();
            featureBuilder.clear();
            // Add geometry
            if (request.getGridType() == GRID_TYPE.GRID) {
                final Rectangle r = GeoTileUtils.toBoundingBox(bucket.getKeyAsString());
                geomBuilder.box(featureBuilder, r.getMinLon(), r.getMaxLon(), r.getMinLat(), r.getMaxLat());
            } else {
                // TODO: it should be the centroid of the data
                final GeoPoint point = (GeoPoint) bucket.getKey();
                geomBuilder.point(featureBuilder, point.lon(), point.lat());
            }
            // Add count as key value pair
            addPropertyToFeature(featureBuilder, layerProps, COUNT_TAG, count);
            // Add aggregations results as key value pair
            for (Aggregation aggregation : bucket.getAggregations()) {
                final String type = aggregation.getType();
                switch (type)  {
                    case MinAggregationBuilder.NAME:
                    case MaxAggregationBuilder.NAME:
                    case AvgAggregationBuilder.NAME:
                    case SumAggregationBuilder.NAME:
                    case CardinalityAggregationBuilder.NAME:
                        final NumericMetricsAggregation.SingleValue metric = (NumericMetricsAggregation.SingleValue) aggregation;
                        addPropertyToFeature(featureBuilder, layerProps, "aggs." + aggregation.getName(), metric.value());
                        break;
                    default:
                        // top term and percentile should be supported
                        throw new IllegalArgumentException("Unknown feature type [" + type + "]");
                }
            }
            aggLayerBuilder.addFeatures(featureBuilder);
        }
        addPropertiesToLayer(aggLayerBuilder, layerProps);
        return aggLayerBuilder;
    }

    private VectorTile.Tile.Layer.Builder getMetaLayer(SearchResponse response, Request request, VectorTileGeometryBuilder geomBuilder) {
        final VectorTile.Tile.Layer.Builder metaLayerBuilder = VectorTileUtils.createLayerBuilder(META_LAYER, request.getExtent());
        final MvtLayerProps layerProps = new MvtLayerProps();
        final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
        final InternalGeoBounds bounds = response.getAggregations() != null ? response.getAggregations().get(BOUNDS_FIELD) : null;
        if (bounds != null && bounds.topLeft() != null) {
            final GeoPoint topLeft = bounds.topLeft();
            final GeoPoint bottomRight = bounds.bottomRight();
            geomBuilder.box(featureBuilder, topLeft.lon(), bottomRight.lon(), bottomRight.lat(), topLeft.lat());
        } else {
            final Rectangle tile = request.getBoundingBox();
            geomBuilder.box(featureBuilder, tile.getMinLon(), tile.getMaxLon(), tile.getMinLat(), tile.getMaxLat());
        }
        addPropertyToFeature(featureBuilder, layerProps, "timed_out", response.isTimedOut());
        addPropertyToFeature(featureBuilder, layerProps, "_shards.total", response.getTotalShards());
        addPropertyToFeature(featureBuilder, layerProps, "_shards.successful", response.getSuccessfulShards());
        addPropertyToFeature(featureBuilder, layerProps, "_shards.skipped", response.getSkippedShards());
        addPropertyToFeature(featureBuilder, layerProps, "_shards.failed", response.getFailedShards());
        addPropertyToFeature(featureBuilder, layerProps, "hits.total.value", response.getHits().getTotalHits().value);
        addPropertyToFeature(featureBuilder, layerProps, "hits.total.relation", response.getHits().getTotalHits().relation.name());
        metaLayerBuilder.addFeatures(featureBuilder);
        addPropertiesToLayer(metaLayerBuilder, layerProps);
        return metaLayerBuilder;
    }

    private void addPropertyToFeature(VectorTile.Tile.Feature.Builder feature, MvtLayerProps layerProps, String key, Object value) {
        feature.addTags(layerProps.addKey(key));
        feature.addTags(layerProps.addValue(value));
    }

    private void addPropertiesToLayer(VectorTile.Tile.Layer.Builder layer, MvtLayerProps layerProps) {
        // Add keys
        layer.addAllKeys(layerProps.getKeys());
        // Add values
        final Iterable<Object> values = layerProps.getVals();
        for (Object value : values) {
            layer.addValues(MvtValue.toValue(value));
        }
    }

    @Override
    public String getName() {
        return "vectortile_action";
    }
}
