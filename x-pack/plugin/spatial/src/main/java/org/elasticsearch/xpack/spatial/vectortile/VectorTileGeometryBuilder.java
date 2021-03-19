/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.vectortile;

import com.wdtinc.mapbox_vector_tile.encoding.GeomCmd;
import com.wdtinc.mapbox_vector_tile.encoding.GeomCmdHdr;
import org.apache.lucene.util.BitUtil;
import org.elasticsearch.geometry.Rectangle;

import java.util.List;

class VectorTileGeometryBuilder {

    private final int extent;
    private final Rectangle rectangle;
    private final double pointXScale, pointYScale;

    VectorTileGeometryBuilder(int z, int x, int y, int extent) {
        this.extent = extent;
        rectangle = VectorTileUtils.getTileBounds(z , x, y);
        pointXScale = 1d / ((rectangle.getMaxLon() - rectangle.getMinLon()) / (double) extent);
        pointYScale = -1d / ((rectangle.getMaxLat() - rectangle.getMinLat()) / (double) extent);
    }

    public int lat(double lat) {
        return (int) (pointYScale * (VectorTileUtils.latToSphericalMercator(lat) - rectangle.getMinY())) + extent;
    }

    public int lon(double lon) {
        return (int) (pointXScale * (VectorTileUtils.lonToSphericalMercator(lon) - rectangle.getMinX()));
    }

    public void point(List<Integer> collector, double lon, double lat) {
        collector.add(GeomCmdHdr.cmdHdr(GeomCmd.MoveTo, 1));
        collector.add(BitUtil.zigZagEncode(lon(lon)));
        collector.add(BitUtil.zigZagEncode(lat(lat)));
    }

    public void box(List<Integer> collector, double minLon, double maxLon, double minLat, double maxLat) {
        final int minX = lon(minLon);
        final int minY = lat(minLat);
        final int maxX = lon(maxLon);
        final int maxY = lat(maxLat);

        collector.add(GeomCmdHdr.cmdHdr(GeomCmd.MoveTo, 1));
        collector.add(BitUtil.zigZagEncode(minX));
        collector.add(BitUtil.zigZagEncode(minY));
        collector.add(GeomCmdHdr.cmdHdr(GeomCmd.LineTo, 3));
        // 1
        collector.add(BitUtil.zigZagEncode(0));
        collector.add(BitUtil.zigZagEncode(maxY - minY));
        // 2
        collector.add(BitUtil.zigZagEncode(maxX - minX));
        collector.add(BitUtil.zigZagEncode(0));
        // 3
        collector.add(BitUtil.zigZagEncode(0));
        collector.add(BitUtil.zigZagEncode(minY - maxY));
        // close
        collector.add(GeomCmdHdr.cmdHdr(GeomCmd.ClosePath, 1));
    }

}
