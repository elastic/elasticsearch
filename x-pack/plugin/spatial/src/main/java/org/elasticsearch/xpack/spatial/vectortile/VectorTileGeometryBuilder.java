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
import org.elasticsearch.geometry.Rectangle;

class VectorTileGeometryBuilder {

    private final int extent;
    private final Rectangle rectangle;
    private final double pointXScale, pointYScale;

    VectorTileGeometryBuilder(int z, int x, int y, int extent) {
        this.extent = extent;
        rectangle = VectorTileUtils.getTileBounds(z, x, y);
        pointXScale = 1d / ((rectangle.getMaxLon() - rectangle.getMinLon()) / (double) extent);
        pointYScale = -1d / ((rectangle.getMaxLat() - rectangle.getMinLat()) / (double) extent);
    }

    public int lat(double lat) {
        return (int) (pointYScale * (VectorTileUtils.latToSphericalMercator(lat) - rectangle.getMinY())) + extent;
    }

    public int lon(double lon) {
        return (int) (pointXScale * (VectorTileUtils.lonToSphericalMercator(lon) - rectangle.getMinX()));
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
        featureBuilder.addGeometry(BitUtil.zigZagEncode(0));
        featureBuilder.addGeometry(BitUtil.zigZagEncode(maxY - minY));
        // 2
        featureBuilder.addGeometry(BitUtil.zigZagEncode(maxX - minX));
        featureBuilder.addGeometry(BitUtil.zigZagEncode(0));
        // 3
        featureBuilder.addGeometry(BitUtil.zigZagEncode(0));
        featureBuilder.addGeometry(BitUtil.zigZagEncode(minY - maxY));
        // close
        featureBuilder.addGeometry(GeomCmdHdr.cmdHdr(GeomCmd.ClosePath, 1));
    }

}
