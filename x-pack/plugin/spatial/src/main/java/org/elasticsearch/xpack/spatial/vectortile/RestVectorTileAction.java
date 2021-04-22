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
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoGridBucket;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoTileGrid;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalGeoBounds;
import org.elasticsearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.MinBucketPipelineAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.profile.SearchProfileShardResults;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestVectorTileAction extends AbstractVectorTileSearchAction<AbstractVectorTileSearchAction.Request> {

    private static final String META_LAYER = "meta";
    private static final String HITS_LAYER = "hits";
    private static final String AGGS_LAYER = "aggs";

    private static final String GRID_FIELD = "grid";
    private static final String BOUNDS_FIELD = "bounds";

    private static final String COUNT_TAG = "_count";
    private static final String ID_TAG = "_id";

    private static final String COUNT_MIN = "_count.min";
    private static final String COUNT_MAX = "_count.max";

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
            final InternalGeoTileGrid grid = s.getAggregations() != null ? s.getAggregations().get(GRID_FIELD) : null;
            final InternalGeoBounds bounds = s.getAggregations() != null ? s.getAggregations().get(BOUNDS_FIELD) : null;
            final Aggregations aggsWithoutGridAndBounds = s.getAggregations() == null
                ? null
                : new Aggregations(
                s.getAggregations()
                    .asList()
                    .stream()
                    .filter(a -> GRID_FIELD.equals(a.getName()) == false && BOUNDS_FIELD.equals(a.getName()) == false)
                    .collect(Collectors.toList())
            );
            SearchResponse meta = new SearchResponse(
                new SearchResponseSections(
                    new SearchHits(SearchHits.EMPTY, s.getHits().getTotalHits(), s.getHits().getMaxScore()), // remove actual hits
                    aggsWithoutGridAndBounds,
                    s.getSuggest(),
                    s.isTimedOut(),
                    s.isTerminatedEarly(),
                    s.getProfileResults() == null ? null : new SearchProfileShardResults(s.getProfileResults()),
                    s.getNumReducePhases()
                ),
                s.getScrollId(),
                s.getTotalShards(),
                s.getSuccessfulShards(),
                s.getSkippedShards(),
                s.getTook().millis(),
                s.getShardFailures(),
                s.getClusters()
            );
            if (hits.length > 0) {
                tileBuilder.addLayers(buildHitsLayer(s, request));
            }
            // TODO: should be expose the total number of buckets on InternalGeoTileGrid?
            if (grid != null && grid.getBuckets().size() > 0) {
                tileBuilder.addLayers(buildAggsLayer(grid, request, geomBuilder));
            }
            tileBuilder.addLayers(buildMetaLayer(meta, bounds, request, geomBuilder));
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
            searchRequestBuilder.addAggregation(new MaxBucketPipelineAggregationBuilder(COUNT_MAX, GRID_FIELD + "._count"));
            searchRequestBuilder.addAggregation(new MinBucketPipelineAggregationBuilder(COUNT_MIN, GRID_FIELD + "._count"));
            final Collection<AggregationBuilder> aggregations = request.getAggregations();
            for (AggregationBuilder aggregation : aggregations) {
                searchRequestBuilder.addAggregation(
                    new MaxBucketPipelineAggregationBuilder(aggregation.getName() + ".max", GRID_FIELD + ">" + aggregation.getName()));
                searchRequestBuilder.addAggregation(
                    new MinBucketPipelineAggregationBuilder(aggregation.getName() + ".min", GRID_FIELD + ">" + aggregation.getName()));
            }
        }
        if (request.getExactBounds()) {
            final GeoBoundsAggregationBuilder boundsBuilder = new GeoBoundsAggregationBuilder(BOUNDS_FIELD).field(request.getField())
                .wrapLongitude(false);
            searchRequestBuilder.addAggregation(boundsBuilder);
        }
        return searchRequestBuilder;
    }

    private VectorTile.Tile.Layer.Builder buildHitsLayer(SearchResponse response, Request request) {
        final FeatureFactory featureFactory = new FeatureFactory(request.getZ(), request.getX(), request.getY(), request.getExtent());
        final GeometryParser parser = new GeometryParser(true, false, false);
        final VectorTile.Tile.Layer.Builder hitsLayerBuilder = VectorTileUtils.createLayerBuilder(HITS_LAYER, request.getExtent());
        final List<FieldAndFormat> fields = request.getFields();
        for (SearchHit searchHit : response.getHits()) {
            final IUserDataConverter tags = (userData, layerProps, featureBuilder) -> {
                // TODO: It would be great if we can add the centroid information for polygons. That information can be
                // used to place labels inside those geometries
                VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, ID_TAG, searchHit.getId());
                if (fields != null) {
                    for (FieldAndFormat field : fields) {
                        final DocumentField documentField = searchHit.field(field.field);
                        if (documentField != null) {
                            VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, field.field, documentField.getValue());
                        }
                    }
                }
            };
            // TODO: See comment on field formats.
            final Geometry geometry = parser.parseGeometry(searchHit.field(request.getField()).getValue());
            hitsLayerBuilder.addAllFeatures(featureFactory.getFeatures(geometry, tags));
        }
        VectorTileUtils.addPropertiesToLayer(hitsLayerBuilder, featureFactory.getLayerProps());
        return hitsLayerBuilder;
    }

    private VectorTile.Tile.Layer.Builder buildAggsLayer(InternalGeoTileGrid grid, Request request, VectorTileGeometryBuilder geomBuilder)
        throws IOException{
        final VectorTile.Tile.Layer.Builder aggLayerBuilder = VectorTileUtils.createLayerBuilder(AGGS_LAYER, request.getExtent());
        final MvtLayerProps layerProps = new MvtLayerProps();
        final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
        for (InternalGeoGridBucket<?> bucket : grid.getBuckets()) {
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
            VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, COUNT_TAG, bucket.getDocCount());
            for (Aggregation aggregation : bucket.getAggregations()) {
                VectorTileUtils.addToXContentToFeature(featureBuilder, layerProps, aggregation);
            }
            aggLayerBuilder.addFeatures(featureBuilder);
        }
        VectorTileUtils.addPropertiesToLayer(aggLayerBuilder, layerProps);
        return aggLayerBuilder;
    }

    private VectorTile.Tile.Layer.Builder buildMetaLayer(
        SearchResponse response,
        InternalGeoBounds bounds,
        Request request,
        VectorTileGeometryBuilder geomBuilder
    ) throws IOException {
        final VectorTile.Tile.Layer.Builder metaLayerBuilder = VectorTileUtils.createLayerBuilder(META_LAYER, request.getExtent());
        final MvtLayerProps layerProps = new MvtLayerProps();
        final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
        if (bounds != null && bounds.topLeft() != null) {
            final GeoPoint topLeft = bounds.topLeft();
            final GeoPoint bottomRight = bounds.bottomRight();
            geomBuilder.box(featureBuilder, topLeft.lon(), bottomRight.lon(), bottomRight.lat(), topLeft.lat());
        } else {
            final Rectangle tile = request.getBoundingBox();
            geomBuilder.box(featureBuilder, tile.getMinLon(), tile.getMaxLon(), tile.getMinLat(), tile.getMaxLat());
        }
        VectorTileUtils.addToXContentToFeature(featureBuilder, layerProps, response);
        metaLayerBuilder.addFeatures(featureBuilder);
        VectorTileUtils.addPropertiesToLayer(metaLayerBuilder, layerProps);
        return metaLayerBuilder;
    }

    @Override
    public String getName() {
        return "vectortile_action";
    }
}
