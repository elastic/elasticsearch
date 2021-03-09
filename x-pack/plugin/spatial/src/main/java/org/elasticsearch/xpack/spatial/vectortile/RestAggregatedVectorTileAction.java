/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.vectortile;


import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.encoding.GeomCmd;
import com.wdtinc.mapbox_vector_tile.encoding.GeomCmdHdr;
import org.apache.lucene.util.BitUtil;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
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
        final SearchRequestBuilder builder = searchBuilder(client, index.split(","), field, z, x, y);
        // TODO: how do we handle cancellations?
        return channel -> builder.execute( new RestResponseListener<>(channel) {

            @Override
            public RestResponse buildResponse(SearchResponse searchResponse) throws IOException {
                Rectangle rectangle = VectorTileUtils.getTileBounds(z , x, y);
                final double pointXScale = 1d / ((rectangle.getMaxLon() - rectangle.getMinLon()) / (double) 256);
                final double pointYScale = -1d / ((rectangle.getMaxLat() - rectangle.getMinLat()) / (double) 256);
                final VectorTile.Tile.Layer.Builder pointLayerBuilder = VectorTile.Tile.Layer.newBuilder();
                pointLayerBuilder.setVersion(2);
                pointLayerBuilder.setName("AGG");
                pointLayerBuilder.setExtent(256);
                pointLayerBuilder.addKeys("count");
                final List<Integer> commands = new ArrayList<>();
                final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
                final VectorTile.Tile.Value.Builder valueBuilder = VectorTile.Tile.Value.newBuilder();
                final HashMap<Long, Integer> values = new HashMap<>();

                final InternalGeoTileGrid t = searchResponse.getAggregations().get(field);
                for (InternalGeoGridBucket<?> bucket : t.getBuckets()) {
                    long count = bucket.getDocCount();
                    if (count > 0) {
                        GeoPoint point = (GeoPoint) bucket.getKey();
                        final int x = (int) (pointXScale * (VectorTileUtils.lonToSphericalMercator(point.lon()) - rectangle.getMinX()));
                        final int y = (int) (pointYScale * (VectorTileUtils.latToSphericalMercator(point.lat()) - rectangle.getMinY())) + 256;
                        featureBuilder.clear();
                        featureBuilder.setType(VectorTile.Tile.GeomType.POINT);
                        // create geometry commands
                        commands.clear();
                        commands.add(GeomCmdHdr.cmdHdr(GeomCmd.MoveTo, 1));
                        commands.add(BitUtil.zigZagEncode(x));
                        commands.add(BitUtil.zigZagEncode(y));
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
                final VectorTile.Tile.Builder tileBuilder = VectorTile.Tile.newBuilder();
                tileBuilder.addLayers(pointLayerBuilder);
                final BytesStream bytesOut = Streams.flushOnCloseStream(channel.bytesOutput());
                bytesOut.write(tileBuilder.build().toByteArray());
                return new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, bytesOut.bytes());
            }
        });
    }

    public static SearchRequestBuilder searchBuilder(Client client, String[] index, String field, int z, int x, int y) throws IOException {
        final Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, z);
        GeoBoundingBox boundingBox =
            new GeoBoundingBox(new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
                new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon()));
        final GeoShapeQueryBuilder qBuilder = QueryBuilders.geoShapeQuery(field, rectangle);
        GeoGridAggregationBuilder aBuilder = new GeoTileGridAggregationBuilder(field)
            .field(field).precision(Math.min(GeoTileUtils.MAX_ZOOM, z + 8))
            .setGeoBoundingBox(boundingBox);
        return client.prepareSearch(index).setQuery(qBuilder).addAggregation(aBuilder).setSize(0);
    }

    @Override
    public String getName() {
        return "vectortile_aggregation_action";
    }
}
