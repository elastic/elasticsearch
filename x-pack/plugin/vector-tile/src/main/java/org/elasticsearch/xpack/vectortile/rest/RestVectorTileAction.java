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
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
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
import org.elasticsearch.usage.SearchUsageHolder;
import org.elasticsearch.xpack.vectortile.feature.FeatureFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.transport.RemoteClusterAware.buildRemoteIndexName;

/**
 * Main class handling a call to the _mvt API.
 */
@ServerlessScope(Scope.PUBLIC)
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
    // internal label position runtime field name
    static final String LABEL_POSITION_FIELD_NAME = INTERNAL_AGG_PREFIX + "label_position";

    private final SearchUsageHolder searchUsageHolder;

    public RestVectorTileAction(SearchUsageHolder searchUsageHolder) {
        this.searchUsageHolder = searchUsageHolder;
    }

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
        final VectorTileRequest request = VectorTileRequest.parseRestRequest(restRequest, searchUsageHolder::updateUsage);
        final SearchRequestBuilder searchRequestBuilder = searchRequestBuilder(cancellableNodeClient, request);
        return channel -> searchRequestBuilder.execute(new RestResponseListener<>(channel) {

            @Override
            public RestResponse buildResponse(SearchResponse searchResponse) throws Exception {
                try (BytesStream bytesOut = Streams.flushOnCloseStream(channel.bytesOutput())) {
                    final FeatureFactory featureFactory = new FeatureFactory(
                        request.getZ(),
                        request.getX(),
                        request.getY(),
                        request.getExtent(),
                        request.getBuffer()
                    );
                    // Even if there is no hits, we return a tile with the meta layer
                    final VectorTile.Tile.Builder tileBuilder = VectorTile.Tile.newBuilder();
                    ensureOpen();
                    final SearchHit[] hits = searchResponse.getHits().getHits();
                    if (hits.length > 0) {
                        tileBuilder.addLayers(buildHitsLayer(hits, request, featureFactory));
                    }
                    ensureOpen();
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
                    final InternalAggregations aggsWithoutGridAndBounds = searchResponse.getAggregations() == null
                        ? null
                        : InternalAggregations.from(
                            searchResponse.getAggregations()
                                .asList()
                                .stream()
                                .filter(a -> GRID_FIELD.equals(a.getName()) == false && BOUNDS_FIELD.equals(a.getName()) == false)
                                .collect(Collectors.toList())
                        );
                    final SearchResponse meta = new SearchResponse(
                        // remove actual hits
                        SearchHits.empty(searchResponse.getHits().getTotalHits(), searchResponse.getHits().getMaxScore()),
                        aggsWithoutGridAndBounds,
                        searchResponse.getSuggest(),
                        searchResponse.isTimedOut(),
                        searchResponse.isTerminatedEarly(),
                        searchResponse.getProfileResults() == null ? null : new SearchProfileResults(searchResponse.getProfileResults()),
                        searchResponse.getNumReducePhases(),
                        searchResponse.getScrollId(),
                        searchResponse.getTotalShards(),
                        searchResponse.getSuccessfulShards(),
                        searchResponse.getSkippedShards(),
                        searchResponse.getTook().millis(),
                        searchResponse.getShardFailures(),
                        searchResponse.getClusters()
                    );
                    try {
                        tileBuilder.addLayers(buildMetaLayer(meta, bounds, request, featureFactory));
                        ensureOpen();
                        tileBuilder.build().writeTo(bytesOut);
                        return new RestResponse(RestStatus.OK, MIME_TYPE, bytesOut.bytes());
                    } finally {
                        meta.decRef();
                    }
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
        Map<String, Object> runtimeMappings = request.getRuntimeMappings();
        if (request.getWithLabels()) {
            // Since we have support for getLabelPosition as a runtime field, we can utilize that here by defining an implicit field
            Map<String, Object> mappings = new HashMap<>();
            if (runtimeMappings.size() > 0) {
                mappings.putAll(runtimeMappings);
            }
            HashMap<String, Object> labelsMap = new HashMap<>();
            labelsMap.put("type", "geo_point");
            labelsMap.put(
                "script",
                "GeoPoint point = doc['" + request.getField() + "'].getLabelPosition(); emit(point.getLat(), point.getLon());"
            );
            mappings.put(LABEL_POSITION_FIELD_NAME, labelsMap);
            searchRequestBuilder.addFetchField(LABEL_POSITION_FIELD_NAME);
            runtimeMappings = mappings;
        }
        searchRequestBuilder.setRuntimeMappings(runtimeMappings);
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
            final GeoBoundingBox boundingBox;
            if (request.getGridAgg().needsBounding(request.getZ(), request.getGridPrecision())) {
                final Rectangle rectangle = request.getBoundingBox();
                boundingBox = new GeoBoundingBox(
                    new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
                    new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
                );
            } else {
                // unbounded
                boundingBox = new GeoBoundingBox(new GeoPoint(Double.NaN, Double.NaN), new GeoPoint(Double.NaN, Double.NaN));
            }
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
            final List<MetricsAggregationBuilder<?>> aggregations = request.getAggBuilder();
            for (MetricsAggregationBuilder<?> aggregation : aggregations) {
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
    private static VectorTile.Tile.Layer.Builder buildHitsLayer(SearchHit[] hits, VectorTileRequest request, FeatureFactory featureFactory)
        throws IOException {
        final VectorTile.Tile.Layer.Builder hitsLayerBuilder = VectorTileUtils.createLayerBuilder(HITS_LAYER, request.getExtent());
        final MvtLayerProps layerProps = new MvtLayerProps();
        final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
        for (SearchHit searchHit : hits) {
            String requestField = request.getField();
            final DocumentField geoField = searchHit.field(requestField);
            if (geoField == null) {
                continue;
            }
            final Map<String, DocumentField> fields = searchHit.getDocumentFields();
            for (Object feature : geoField) {
                featureBuilder.clear();
                featureBuilder.mergeFrom((byte[]) feature);
                VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, ID_TAG, searchHit.getId());
                VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, INDEX_TAG, buildQualifiedIndex(searchHit));
                addHitsFields(featureBuilder, layerProps, requestField, fields);
                hitsLayerBuilder.addFeatures(featureBuilder);
            }
            if (request.getWithLabels()) {
                final DocumentField labelField = searchHit.field(LABEL_POSITION_FIELD_NAME);
                if (labelField != null) {
                    Object labelPosValue = labelField.getValue();
                    GeoPoint labelPos = GeoUtils.parseGeoPoint(labelPosValue, true);
                    byte[] labelPosFeature = featureFactory.point(labelPos.lon(), labelPos.lat());
                    if (labelPosFeature != null && labelPosFeature.length != 0) {
                        featureBuilder.clear();
                        featureBuilder.mergeFrom(labelPosFeature);
                        VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, ID_TAG, searchHit.getId());
                        VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, INDEX_TAG, buildQualifiedIndex(searchHit));
                        VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, LABEL_POSITION_FIELD_NAME, true);
                        addHitsFields(featureBuilder, layerProps, requestField, fields);
                        hitsLayerBuilder.addFeatures(featureBuilder);
                    }
                }
            }
        }
        VectorTileUtils.addPropertiesToLayer(hitsLayerBuilder, layerProps);
        return hitsLayerBuilder;
    }

    private static void addHitsFields(
        final VectorTile.Tile.Feature.Builder featureBuilder,
        final MvtLayerProps layerProps,
        String requestField,
        Map<String, DocumentField> fields
    ) {
        for (String field : fields.keySet()) {
            if ((requestField.equals(field) == false) && (field.equals(LABEL_POSITION_FIELD_NAME) == false)) {
                VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, field, fields.get(field).getValue());
            }
        }
    }

    private static String buildQualifiedIndex(SearchHit hit) {
        return buildRemoteIndexName(hit.getClusterAlias(), hit.getIndex());
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
            final byte[] feature = request.getGridType().toFeature(request.getGridAgg(), bucket, bucketKey, featureFactory);
            if (feature == null) {
                // It can only happen in GeoHexAggregation because hex bins are not aligned with the tiles.
                assert request.getGridAgg() == GridAggregation.GEOHEX;
                continue;
            }
            // Add geometry
            featureBuilder.mergeFrom(feature);
            // Add bucket key as key value pair
            VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, KEY_TAG, bucketKey);
            // Add count as key value pair
            VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, COUNT_TAG, bucket.getDocCount());
            // Add all aggregation results
            addAggsFields(featureBuilder, layerProps, bucket);
            // Build the feature
            aggLayerBuilder.addFeatures(featureBuilder);

            if (request.getWithLabels()) {
                // Add label position as point
                featureBuilder.clear();
                GeoPoint labelPos = (GeoPoint) bucket.getKey();
                byte[] labelPosFeature = featureFactory.point(labelPos.lon(), labelPos.lat());
                if (labelPosFeature != null && labelPosFeature.length != 0) {
                    featureBuilder.mergeFrom(labelPosFeature);
                    VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, KEY_TAG, bucketKey);
                    VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, COUNT_TAG, bucket.getDocCount());
                    VectorTileUtils.addPropertyToFeature(featureBuilder, layerProps, LABEL_POSITION_FIELD_NAME, true);
                    addAggsFields(featureBuilder, layerProps, bucket);
                    aggLayerBuilder.addFeatures(featureBuilder);
                }
            }
        }
        VectorTileUtils.addPropertiesToLayer(aggLayerBuilder, layerProps);
        return aggLayerBuilder;
    }

    private static void addAggsFields(
        final VectorTile.Tile.Feature.Builder featureBuilder,
        final MvtLayerProps layerProps,
        final InternalGeoGridBucket bucket
    ) throws IOException {
        for (Aggregation aggregation : bucket.getAggregations()) {
            if (aggregation.getName().startsWith(INTERNAL_AGG_PREFIX) == false) {
                VectorTileUtils.addToXContentToFeature(featureBuilder, layerProps, aggregation);
            }
        }
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
        VectorTileUtils.addToXContentToFeature(featureBuilder, layerProps, ChunkedToXContent.wrapAsToXContent(response));
        metaLayerBuilder.addFeatures(featureBuilder);
        VectorTileUtils.addPropertiesToLayer(metaLayerBuilder, layerProps);
        return metaLayerBuilder;
    }
}
