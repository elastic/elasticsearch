/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.vectortile.rest;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerProps;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.SimpleFeatureFactory;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoGridBucket;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoTileGrid;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalGeoBounds;
import org.elasticsearch.search.aggregations.pipeline.StatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Main class handling a call to the _mvt API.
 */
public class RestVectorTileAction extends BaseRestHandler {

    private static final String META_LAYER = "meta";
    private static final String HITS_LAYER = "hits";
    private static final String AGGS_LAYER = "aggs";

    private static final String GRID_FIELD = "grid";
    private static final String BOUNDS_FIELD = "bounds";

    private static final String COUNT_TAG = "_count";
    private static final String ID_TAG = "_id";

    // mime type as defined by the mapbox vector tile specification
    private static final String MIME_TYPE = "application/vnd.mapbox-vector-tile";

    public RestVectorTileAction() {}

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "{index}/_mvt/{field}/{z}/{x}/{y}"), new Route(POST, "{index}/_mvt/{field}/{z}/{x}/{y}"));
    }

    @Override
    public String getName() {
        return "vector_tile_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        // This will allow to cancel the search request if the http channel is closed
        final RestCancellableNodeClient cancellableNodeClient = new RestCancellableNodeClient(client, restRequest.getHttpChannel());
        final VectorTileRequest request = VectorTileRequest.parseRestRequest(restRequest);
        final SearchRequestBuilder searchRequestBuilder = searchRequestBuilder(cancellableNodeClient, request);
        return channel -> searchRequestBuilder.execute(new RestResponseListener<>(channel) {

            @Override
            public RestResponse buildResponse(SearchResponse searchResponse) throws Exception {
                try (BytesStream bytesOut = Streams.flushOnCloseStream(channel.bytesOutput())) {
                    // Even if there is no hits, we return a tile with the meta layer
                    final VectorTile.Tile.Builder tileBuilder = VectorTile.Tile.newBuilder();
                    ensureOpen();
                    final SearchHit[] hits = searchResponse.getHits().getHits();
                    if (hits.length > 0) {
                        tileBuilder.addLayers(buildHitsLayer(hits, request));
                    }
                    ensureOpen();
                    final SimpleFeatureFactory geomBuilder = new SimpleFeatureFactory(
                        request.getZ(),
                        request.getX(),
                        request.getY(),
                        request.getExtent()
                    );
                    final InternalGeoTileGrid grid = searchResponse.getAggregations() != null
                        ? searchResponse.getAggregations().get(GRID_FIELD)
                        : null;
                    // TODO: should we expose the total number of buckets on InternalGeoTileGrid?
                    if (grid != null && grid.getBuckets().size() > 0) {
                        tileBuilder.addLayers(buildAggsLayer(grid, request, geomBuilder));
                    }
                    ensureOpen();
                    final InternalGeoBounds bounds = searchResponse.getAggregations() != null
                        ? searchResponse.getAggregations().get(BOUNDS_FIELD)
                        : null;
                    final Aggregations aggsWithoutGridAndBounds = searchResponse.getAggregations() == null
                        ? null
                        : new Aggregations(
                            searchResponse.getAggregations()
                                .asList()
                                .stream()
                                .filter(a -> GRID_FIELD.equals(a.getName()) == false && BOUNDS_FIELD.equals(a.getName()) == false)
                                .collect(Collectors.toList())
                        );
                    final SearchResponse meta = new SearchResponse(
                        new SearchResponseSections(
                            new SearchHits(
                                SearchHits.EMPTY,
                                searchResponse.getHits().getTotalHits(),
                                searchResponse.getHits().getMaxScore()
                            ), // remove actual hits
                            aggsWithoutGridAndBounds,
                            searchResponse.getSuggest(),
                            searchResponse.isTimedOut(),
                            searchResponse.isTerminatedEarly(),
                            searchResponse.getProfileResults() == null
                                ? null
                                : new SearchProfileResults(searchResponse.getProfileResults()),
                            searchResponse.getNumReducePhases()
                        ),
                        searchResponse.getScrollId(),
                        searchResponse.getTotalShards(),
                        searchResponse.getSuccessfulShards(),
                        searchResponse.getSkippedShards(),
                        searchResponse.getTook().millis(),
                        searchResponse.getShardFailures(),
                        searchResponse.getClusters()
                    );
                    tileBuilder.addLayers(buildMetaLayer(meta, bounds, request, geomBuilder));
                    ensureOpen();
                    tileBuilder.build().writeTo(bytesOut);
                    return new BytesRestResponse(RestStatus.OK, MIME_TYPE, bytesOut.bytes());
                }
            }
        });
    }

    private static SearchRequestBuilder searchRequestBuilder(RestCancellableNodeClient client, VectorTileRequest request)
        throws IOException {
        final SearchRequestBuilder searchRequestBuilder = client.prepareSearch(request.getIndexes());
        searchRequestBuilder.setSize(request.getSize());
        searchRequestBuilder.setFetchSource(false);
        searchRequestBuilder.addFetchField(
            new FieldAndFormat(
                request.getField(),
                "mvt(" + request.getZ() + "/" + request.getX() + "/" + request.getY() + "@" + request.getExtent() + ")"
            )
        );
        for (FieldAndFormat field : request.getFieldAndFormats()) {
            searchRequestBuilder.addFetchField(field);
        }
        searchRequestBuilder.setRuntimeMappings(request.getRuntimeMappings());
        QueryBuilder qBuilder = QueryBuilders.geoShapeQuery(request.getField(), request.getBoundingBox());
        if (request.getQueryBuilder() != null) {
            final BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            boolQueryBuilder.filter(request.getQueryBuilder());
            boolQueryBuilder.filter(qBuilder);
            qBuilder = boolQueryBuilder;
        }
        searchRequestBuilder.setQuery(qBuilder);
        if (request.getGridPrecision() > 0) {
            final Rectangle rectangle = request.getBoundingBox();
            final GeoBoundingBox boundingBox = new GeoBoundingBox(
                new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
                new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
            );
            final int extent = 1 << request.getGridPrecision();
            final GeoGridAggregationBuilder tileAggBuilder = new GeoTileGridAggregationBuilder(GRID_FIELD).field(request.getField())
                .precision(Math.min(GeoTileUtils.MAX_ZOOM, request.getZ() + request.getGridPrecision()))
                .setGeoBoundingBox(boundingBox)
                .size(extent * extent);
            searchRequestBuilder.addAggregation(tileAggBuilder);
            searchRequestBuilder.addAggregation(new StatsBucketPipelineAggregationBuilder(COUNT_TAG, GRID_FIELD + "." + COUNT_TAG));
            final AggregatorFactories.Builder otherAggBuilder = request.getAggBuilder();
            if (otherAggBuilder != null) {
                tileAggBuilder.subAggregations(request.getAggBuilder());
                final Collection<AggregationBuilder> aggregations = otherAggBuilder.getAggregatorFactories();
                for (AggregationBuilder aggregation : aggregations) {
                    searchRequestBuilder.addAggregation(
                        new StatsBucketPipelineAggregationBuilder(aggregation.getName(), GRID_FIELD + ">" + aggregation.getName())
                    );
                }
            }
        }
        if (request.getExactBounds()) {
            final GeoBoundsAggregationBuilder boundsBuilder = new GeoBoundsAggregationBuilder(BOUNDS_FIELD).field(request.getField())
                .wrapLongitude(false);
            searchRequestBuilder.addAggregation(boundsBuilder);
        }
        for (SortBuilder<?> sortBuilder : request.getSortBuilders()) {
            searchRequestBuilder.addSort(sortBuilder);
        }
        return searchRequestBuilder;
    }

    @SuppressWarnings("unchecked")
    private static VectorTile.Tile.Layer.Builder buildHitsLayer(SearchHit[] hits, VectorTileRequest request) throws IOException {
        final VectorTile.Tile.Layer.Builder hitsLayerBuilder = VectorTileUtils.createLayerBuilder(HITS_LAYER, request.getExtent());
        final List<FieldAndFormat> fields = request.getFieldAndFormats();
        final MvtLayerProps layerProps = new MvtLayerProps();
        final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
        for (SearchHit searchHit : hits) {
            final DocumentField geoField = searchHit.field(request.getField());
            if (geoField == null) {
                continue;
            }
            for (Object feature : geoField) {
                featureBuilder.clear();
                featureBuilder.mergeFrom((byte[]) feature);
                VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, ID_TAG, searchHit.getId());
                if (fields != null) {
                    for (FieldAndFormat field : fields) {
                        final DocumentField documentField = searchHit.field(field.field);
                        if (documentField != null) {
                            VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, field.field, documentField.getValue());
                        }
                    }
                }
                hitsLayerBuilder.addFeatures(featureBuilder);
            }
        }
        VectorTileUtils.addPropertiesToLayer(hitsLayerBuilder, layerProps);
        return hitsLayerBuilder;
    }

    private static VectorTile.Tile.Layer.Builder buildAggsLayer(
        InternalGeoTileGrid grid,
        VectorTileRequest request,
        SimpleFeatureFactory geomBuilder
    ) throws IOException {
        final VectorTile.Tile.Layer.Builder aggLayerBuilder = VectorTileUtils.createLayerBuilder(AGGS_LAYER, request.getExtent());
        final MvtLayerProps layerProps = new MvtLayerProps();
        final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
        for (InternalGeoGridBucket bucket : grid.getBuckets()) {
            featureBuilder.clear();
            // Add geometry
            if (request.getGridType() == VectorTileRequest.GRID_TYPE.GRID) {
                final Rectangle r = GeoTileUtils.toBoundingBox(bucket.getKeyAsString());
                featureBuilder.mergeFrom(geomBuilder.box(r.getMinLon(), r.getMaxLon(), r.getMinLat(), r.getMaxLat()));
            } else {
                // TODO: it should be the centroid of the data?
                final GeoPoint point = (GeoPoint) bucket.getKey();
                featureBuilder.mergeFrom(geomBuilder.point(point.lon(), point.lat()));
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

    private static VectorTile.Tile.Layer.Builder buildMetaLayer(
        SearchResponse response,
        InternalGeoBounds bounds,
        VectorTileRequest request,
        SimpleFeatureFactory geomBuilder
    ) throws IOException {
        final VectorTile.Tile.Layer.Builder metaLayerBuilder = VectorTileUtils.createLayerBuilder(META_LAYER, request.getExtent());
        final MvtLayerProps layerProps = new MvtLayerProps();
        final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
        if (bounds != null && bounds.topLeft() != null) {
            final GeoPoint topLeft = bounds.topLeft();
            final GeoPoint bottomRight = bounds.bottomRight();
            featureBuilder.mergeFrom(geomBuilder.box(topLeft.lon(), bottomRight.lon(), bottomRight.lat(), topLeft.lat()));
        } else {
            final Rectangle tile = request.getBoundingBox();
            featureBuilder.mergeFrom(geomBuilder.box(tile.getMinLon(), tile.getMaxLon(), tile.getMinLat(), tile.getMaxLat()));
        }
        VectorTileUtils.addToXContentToFeature(featureBuilder, layerProps, response);
        metaLayerBuilder.addFeatures(featureBuilder);
        VectorTileUtils.addPropertiesToLayer(metaLayerBuilder, layerProps);
        return metaLayerBuilder;
    }
}
