/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.vectortile;


import com.wdtinc.mapbox_vector_tile.VectorTile;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoGridBucket;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoTileGrid;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalGeoBounds;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestAggregatedVectorTileAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "{index}/_agg_mvt/{field}/{z}/{x}/{y}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        final String index  = restRequest.param("index");
        final String field  = restRequest.param("field");
        final int z = Integer.parseInt(restRequest.param("z"));
        final int x = Integer.parseInt(restRequest.param("x"));
        final int y = Integer.parseInt(restRequest.param("y"));
        final boolean isGrid = restRequest.hasParam("type") ? "grid".equals(restRequest.param("type")) : false;

        final VectorTileAggConfig config = resolveConfig(restRequest);
        final SearchRequestBuilder builder = searchBuilder(client, index.split(","), field, z, x, y, config);
        final int extent =  1 << config.getScaling();
        // TODO: how do we handle cancellations?
        return channel -> builder.execute( new RestResponseListener<>(channel) {

            @Override
            public RestResponse buildResponse(SearchResponse searchResponse) throws IOException {
                // TODO: of there is no hits, should we return an empty tile with no layers or
                // a tile with empty layers?
                final VectorTile.Tile.Builder tileBuilder = VectorTile.Tile.newBuilder();
                final VectorTileGeometryBuilder geomBuilder = new VectorTileGeometryBuilder(z, x, y, extent);
                final InternalGeoTileGrid t = searchResponse.getAggregations().get("grid");
                tileBuilder.addLayers(getPointLayer(t, geomBuilder));
                final InternalGeoBounds b = searchResponse.getAggregations().get("bounds");
                tileBuilder.addLayers(getMetaLayer(b, geomBuilder));
                final BytesStream bytesOut = Streams.flushOnCloseStream(channel.bytesOutput());
                bytesOut.write(tileBuilder.build().toByteArray());
                return new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, bytesOut.bytes());
            }

            private VectorTile.Tile.Layer.Builder getPointLayer(InternalGeoTileGrid t, VectorTileGeometryBuilder geomBuilder) {
                final VectorTile.Tile.Layer.Builder pointLayerBuilder = VectorTile.Tile.Layer.newBuilder();
                pointLayerBuilder.setVersion(2);
                pointLayerBuilder.setName("AGG");
                pointLayerBuilder.setExtent(extent);
                pointLayerBuilder.addKeys("count");
                final List<Integer> commands = new ArrayList<>();
                final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
                final VectorTile.Tile.Value.Builder valueBuilder = VectorTile.Tile.Value.newBuilder();
                final HashMap<Long, Integer> values = new HashMap<>();

                for (InternalGeoGridBucket<?> bucket : t.getBuckets()) {
                    long count = bucket.getDocCount();
                    if (count > 0) {
                        featureBuilder.clear();
                        // create geometry commands
                        commands.clear();
                        if (isGrid) {
                            featureBuilder.setType(VectorTile.Tile.GeomType.POLYGON);
                            Rectangle r = GeoTileUtils.toBoundingBox(bucket.getKeyAsString());
                            geomBuilder.box(commands, r.getMinLon(), r.getMaxLon(), r.getMinLat(), r.getMaxLat());
                        } else {
                            featureBuilder.setType(VectorTile.Tile.GeomType.POINT);
                            GeoPoint point = (GeoPoint) bucket.getKey();
                            geomBuilder.point(commands, point.lon(), point.lat());
                        }
                        featureBuilder.addAllGeometry(commands);
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

            private VectorTile.Tile.Layer.Builder getMetaLayer(InternalGeoBounds t, VectorTileGeometryBuilder geomBuilder) {
                final VectorTile.Tile.Layer.Builder metaLayerBuilder = VectorTile.Tile.Layer.newBuilder();
                metaLayerBuilder.setVersion(2);
                metaLayerBuilder.setName("META");
                metaLayerBuilder.setExtent(extent);
                final GeoPoint topLeft = t.topLeft();
                final GeoPoint bottomRight = t.bottomRight();
                if (topLeft != null && bottomRight != null) {
                    final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
                    featureBuilder.setType(VectorTile.Tile.GeomType.POLYGON);
                    final List<Integer> commands = new ArrayList<>();
                    geomBuilder.box(commands, topLeft.lon(), bottomRight.lon(), bottomRight.lat(), topLeft.lat());
                    featureBuilder.addAllGeometry(commands);
                    metaLayerBuilder.addFeatures(featureBuilder);
                }
                return metaLayerBuilder;
            }
        });
    }

    public static SearchRequestBuilder searchBuilder(Client client, String[] index, String field, int z, int x, int y,
                                                     VectorTileAggConfig config) throws IOException {
        final Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, z);
        GeoBoundingBox boundingBox =
            new GeoBoundingBox(new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
                new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon()));
        QueryBuilder qBuilder = QueryBuilders.geoShapeQuery(field, rectangle);
        if (config.getQueryBuilder() != null) {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            boolQueryBuilder.filter(config.getQueryBuilder());
            boolQueryBuilder.filter(qBuilder);
            qBuilder = boolQueryBuilder;
        }
        int extent = 1 << config.getScaling();
        GeoGridAggregationBuilder aBuilder = new GeoTileGridAggregationBuilder("grid")
            .field(field).precision(Math.min(GeoTileUtils.MAX_ZOOM, z + config.getScaling()))
            .setGeoBoundingBox(boundingBox).size(extent * extent);
        if (config.getAggBuilder() != null) {
            aBuilder.subAggregations(config.getAggBuilder());
        }
        GeoBoundsAggregationBuilder boundsBuilder = new GeoBoundsAggregationBuilder("bounds").field(field).wrapLongitude(false);
        SearchRequestBuilder requestBuilder =  client.prepareSearch(index).setQuery(qBuilder)
            .addAggregation(aBuilder).addAggregation(boundsBuilder).setSize(0);
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
