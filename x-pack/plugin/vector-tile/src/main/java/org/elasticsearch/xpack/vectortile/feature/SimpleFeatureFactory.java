/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile.feature;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.encoding.GeomCmd;
import com.wdtinc.mapbox_vector_tile.encoding.GeomCmdHdr;
import org.apache.lucene.util.BitUtil;
import org.elasticsearch.geometry.Rectangle;

/**
 * Similar to {@link FeatureFactory} but only supports points and rectangles. It is just
 * much more efficient for those shapes.
 */
public class SimpleFeatureFactory {

    private final int extent;
    private final Rectangle rectangle;
    private final double pointXScale, pointYScale;

    public SimpleFeatureFactory(int z, int x, int y, int extent) {
        this.extent = extent;
        rectangle = FeatureFactoryUtils.getTileBounds(z, x, y);
        pointXScale = (double) extent / (rectangle.getMaxLon() - rectangle.getMinLon());
        pointYScale = -(double) extent / (rectangle.getMaxLat() - rectangle.getMinLat());
    }

    public void point(VectorTile.Tile.Feature.Builder featureBuilder, double lon, double lat) {
        featureBuilder.setType(VectorTile.Tile.GeomType.POINT);
        featureBuilder.addGeometry(GeomCmdHdr.cmdHdr(GeomCmd.MoveTo, 1));
        featureBuilder.addGeometry(BitUtil.zigZagEncode(lon(lon)));
        featureBuilder.addGeometry(BitUtil.zigZagEncode(lat(lat)));
    }

    public void box(VectorTile.Tile.Feature.Builder featureBuilder, double minLon, double maxLon, double minLat, double maxLat) {
        featureBuilder.setType(VectorTile.Tile.GeomType.POLYGON);
        final int minX = lon(minLon);
        final int minY = lat(minLat);
        final int maxX = lon(maxLon);
        final int maxY = lat(maxLat);
        featureBuilder.addGeometry(GeomCmdHdr.cmdHdr(GeomCmd.MoveTo, 1));
        featureBuilder.addGeometry(BitUtil.zigZagEncode(minX));
        featureBuilder.addGeometry(BitUtil.zigZagEncode(minY));
        featureBuilder.addGeometry(GeomCmdHdr.cmdHdr(GeomCmd.LineTo, 3));
        // 1
        featureBuilder.addGeometry(BitUtil.zigZagEncode(maxX - minX));
        featureBuilder.addGeometry(BitUtil.zigZagEncode(0));
        // 2
        featureBuilder.addGeometry(BitUtil.zigZagEncode(0));
        featureBuilder.addGeometry(BitUtil.zigZagEncode(maxY - minY));
        // 3
        featureBuilder.addGeometry(BitUtil.zigZagEncode(minX - maxX));
        featureBuilder.addGeometry(BitUtil.zigZagEncode(0));
        // close
        featureBuilder.addGeometry(GeomCmdHdr.cmdHdr(GeomCmd.ClosePath, 1));
    }

    private int lat(double lat) {
        return (int) Math.round(pointYScale * (FeatureFactoryUtils.latToSphericalMercator(lat) - rectangle.getMinY())) + extent;
    }

    private int lon(double lon) {
        return (int) Math.round(pointXScale * (FeatureFactoryUtils.lonToSphericalMercator(lon) - rectangle.getMinX()));
    }
}
