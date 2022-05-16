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
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
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
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoGridBucket;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoCentroidAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalGeoBounds;
import org.elasticsearch.search.aggregations.pipeline.StatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder.MetricsAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xpack.vectortile.feature.FeatureFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private static final String INDEX_TAG = "_index";
    private static final String KEY_TAG = "_key";

    // mime type as defined by the mapbox vector tile specification
    private static final String MIME_TYPE = "application/vnd.mapbox-vector-tile";
    // prefox for internal aggregations. User aggregations cannot start with this prefix
    private static final String INTERNAL_AGG_PREFIX = "_mvt_";
    // internal centroid aggregation name
    static final String CENTROID_AGG_NAME = INTERNAL_AGG_PREFIX + "centroid";

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
                    final FeatureFactory featureFactory = new FeatureFactory(
                        request.getZ(),
                        request.getX(),
                        request.getY(),
                        request.getExtent(),
                        request.getBuffer()
                    );
                    final InternalGeoGrid<?> grid = searchResponse.getAggregations() != null
                        ? searchResponse.getAggregations().get(GRID_FIELD)
                        : null;
                    // TODO: should we expose the total number of buckets on InternalGeoTileGrid?
                    if (grid != null && grid.getBuckets().size() > 0) {
                        tileBuilder.addLayers(buildAggsLayer(grid, request, featureFactory));
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
                    tileBuilder.addLayers(buildMetaLayer(meta, bounds, request, featureFactory));
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
        searchRequestBuilder.setTrackTotalHitsUpTo(request.getTrackTotalHitsUpTo());
        for (FieldAndFormat field : request.getFieldAndFormats()) {
            searchRequestBuilder.addFetchField(field);
        }
        // added last in case there is a wildcard, the last one is picked
        String args = request.getZ() + "/" + request.getX() + "/" + request.getY() + "@" + request.getExtent() + ":" + request.getBuffer();
        searchRequestBuilder.addFetchField(new FieldAndFormat(request.getField(), "mvt(" + args + ")"));
        searchRequestBuilder.setRuntimeMappings(request.getRuntimeMappings());
        // For Hex aggregation we might need to buffer the bounding box
        final Rectangle boxFilter = request.getGridAgg().bufferTile(request.getBoundingBox(), request.getZ(), request.getGridPrecision());
        QueryBuilder qBuilder = QueryBuilders.geoShapeQuery(request.getField(), boxFilter);
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
            final GeoGridAggregationBuilder tileAggBuilder = request.getGridAgg()
                .newAgg(GRID_FIELD)
                .field(request.getField())
                .precision(request.getGridAgg().gridPrecisionToAggPrecision(request.getZ(), request.getGridPrecision()))
                .setGeoBoundingBox(boundingBox)
                .size(MultiBucketConsumerService.DEFAULT_MAX_BUCKETS);

            searchRequestBuilder.addAggregation(tileAggBuilder);
            searchRequestBuilder.addAggregation(new StatsBucketPipelineAggregationBuilder(COUNT_TAG, GRID_FIELD + "." + COUNT_TAG));
            if (request.getGridType() == GridType.CENTROID) {
                tileAggBuilder.subAggregation(new GeoCentroidAggregationBuilder(CENTROID_AGG_NAME).field(request.getField()));
            }
            final List<MetricsAggregationBuilder<?, ?>> aggregations = request.getAggBuilder();
            for (MetricsAggregationBuilder<?, ?> aggregation : aggregations) {
                if (aggregation.getName().startsWith(INTERNAL_AGG_PREFIX)) {
                    throw new IllegalArgumentException(
                        "Invalid aggregation name ["
                            + aggregation.getName()
                            + "]. Aggregation names cannot start with prefix '"
                            + INTERNAL_AGG_PREFIX
                            + "'"
                    );
                }
                tileAggBuilder.subAggregation(aggregation);
                final Set<String> metricNames = aggregation.metricNames();
                for (String metric : metricNames) {
                    final String bucketPath;
                    // handle the case where the metric contains a dot
                    if (metric.contains(".")) {
                        bucketPath = GRID_FIELD + ">" + aggregation.getName() + "[" + metric + "]";
                    } else {
                        bucketPath = GRID_FIELD + ">" + aggregation.getName() + "." + metric;
                    }
                    // we only add the metric name to multi-value metric aggregations
                    final String aggName = metricNames.size() == 1 ? aggregation.getName() : aggregation.getName() + "." + metric;
                    searchRequestBuilder.addAggregation(new StatsBucketPipelineAggregationBuilder(aggName, bucketPath));

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
                VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, INDEX_TAG, searchHit.getIndex());
                final Map<String, DocumentField> fields = searchHit.getDocumentFields();
                for (String field : fields.keySet()) {
                    if (request.getField().equals(field) == false) {
                        VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, field, fields.get(field).getValue());
                    }
                }
                hitsLayerBuilder.addFeatures(featureBuilder);
            }
        }
        VectorTileUtils.addPropertiesToLayer(hitsLayerBuilder, layerProps);
        return hitsLayerBuilder;
    }

    private static VectorTile.Tile.Layer.Builder buildAggsLayer(
        InternalGeoGrid<?> grid,
        VectorTileRequest request,
        FeatureFactory featureFactory
    ) throws IOException {
        final VectorTile.Tile.Layer.Builder aggLayerBuilder = VectorTileUtils.createLayerBuilder(AGGS_LAYER, request.getExtent());
        final MvtLayerProps layerProps = new MvtLayerProps();
        final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
        for (InternalGeoGridBucket bucket : grid.getBuckets()) {
            featureBuilder.clear();
            final String bucketKey = bucket.getKeyAsString();
            // Add geometry
            final byte[] feature = request.getGridType().toFeature(request.getGridAgg(), bucket, bucketKey, featureFactory);
            if (feature != null) {
                featureBuilder.mergeFrom(feature);
            } else {
                // It can only happen in GeoHexAggregation because hex bins are not aligned with the tiles.
                assert request.getGridAgg() == GridAggregation.GEOHEX;
                continue;
            }
            // Add bucket key as key value pair
            VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, KEY_TAG, bucketKey);
            // Add count as key value pair
            VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, COUNT_TAG, bucket.getDocCount());
            for (Aggregation aggregation : bucket.getAggregations()) {
                if (aggregation.getName().startsWith(INTERNAL_AGG_PREFIX) == false) {
                    VectorTileUtils.addToXContentToFeature(featureBuilder, layerProps, aggregation);
                }
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
        FeatureFactory featureFactory
    ) throws IOException {
        final VectorTile.Tile.Layer.Builder metaLayerBuilder = VectorTileUtils.createLayerBuilder(META_LAYER, request.getExtent());
        final MvtLayerProps layerProps = new MvtLayerProps();
        final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
        if (bounds != null && bounds.topLeft() != null) {
            final GeoPoint topLeft = bounds.topLeft();
            final GeoPoint bottomRight = bounds.bottomRight();
            featureBuilder.mergeFrom(featureFactory.box(topLeft.lon(), bottomRight.lon(), bottomRight.lat(), topLeft.lat()));
        } else {
            final Rectangle tile = request.getBoundingBox();
            featureBuilder.mergeFrom(featureFactory.box(tile.getMinLon(), tile.getMaxLon(), tile.getMinLat(), tile.getMaxLat()));
        }
        VectorTileUtils.addToXContentToFeature(featureBuilder, layerProps, response);
        metaLayerBuilder.addFeatures(featureBuilder);
        VectorTileUtils.addPropertiesToLayer(metaLayerBuilder, layerProps);
        return metaLayerBuilder;
    }
}
