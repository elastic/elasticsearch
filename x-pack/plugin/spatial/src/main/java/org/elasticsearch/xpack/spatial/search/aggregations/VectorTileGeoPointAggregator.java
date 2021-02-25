/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.encoding.GeomCmd;
import com.wdtinc.mapbox_vector_tile.encoding.GeomCmdHdr;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BitUtil;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.spatial.vectortile.VectorTileUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class VectorTileGeoPointAggregator extends AbstractVectorTileAggregator {

    private static int extent = 256;
    private final double height;
    private final double width;
    private final double minX;
    private final double minY;

    public VectorTileGeoPointAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        int z,
        int x,
        int y,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, valuesSourceConfig, z, x, y, extent, context, parent, metadata);
        Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, z);
        this.minX = VectorTileUtils.lonToSphericalMercator(rectangle.getMinLon());
        this.minY = VectorTileUtils.latToSphericalMercator(rectangle.getMinLat());
        this.height = VectorTileUtils.latToSphericalMercator(rectangle.getMaxLat()) - minY;
        this.width = VectorTileUtils.lonToSphericalMercator(rectangle.getMaxLon()) - minX;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) {
        final MultiGeoPointValues values = ((ValuesSource.GeoPoint) valuesSource).geoPointValues(ctx);
        final double xScale = 1d / (width / (double) extent);
        final double yScale = -1d / (height / (double) extent);
        final List<Integer> commands = new ArrayList<>();
        final VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    commands.clear();
                    int numPoints = values.docValueCount();
                    if (numPoints == 1) {
                        GeoPoint point = values.nextValue();
                        int x = (int) (xScale * (VectorTileUtils.lonToSphericalMercator(point.lon()) - minX));
                        int y = (int) (yScale * (VectorTileUtils.latToSphericalMercator(point.lat()) - minY)) + extent;
                        if (x >= 0 && x <= extent && y >= 0 && y <= extent) {
                            commands.add(GeomCmdHdr.cmdHdr(GeomCmd.MoveTo, 1));
                            commands.add(BitUtil.zigZagEncode(x));
                            commands.add(BitUtil.zigZagEncode(y));
                        }
                    } else {
                        int prevX = 0;
                        int prevY = 0;
                        commands.add(GeomCmdHdr.cmdHdr(GeomCmd.MoveTo, numPoints));
                        for (int i = 0; i < numPoints; i++) {
                            GeoPoint point = values.nextValue();
                            int x = (int) (xScale * (VectorTileUtils.lonToSphericalMercator(point.lon()) - minX));
                            int y = (int) (yScale * (VectorTileUtils.latToSphericalMercator(point.lat()) - minY)) + extent;
                            if (x >= 0 && x <= extent && y >= 0 && y <= extent) {
                                commands.add(BitUtil.zigZagEncode(x - prevX));
                                commands.add(BitUtil.zigZagEncode(y - prevY));
                                prevX = x;
                                prevY = y;
                            }
                        }
                    }
                    if (commands.size() > 0) {
                        featureBuilder.clear();
                        featureBuilder.setType(VectorTile.Tile.GeomType.POINT);
                        featureBuilder.addAllGeometry(commands);
                        layerBuilder.addFeatures(featureBuilder);
                    }
                }
            }
        };
    }
}
