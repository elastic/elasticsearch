/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.apache.lucene.util.BitUtil;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;

/**
 * Transforms points and rectangles objects in WGS84 into mvt features.
 */
public class SimpleFeatureFactory {

    private final int extent;
    private final double pointXScale, pointYScale, pointXTranslate, pointYTranslate;

    private static final int MOVETO = 1;
    private static final int LINETO = 2;
    private static final int CLOSEPATH = 7;

    private static final byte[] EMPTY = new byte[0];

    public SimpleFeatureFactory(int z, int x, int y, int extent) {
        this.extent = extent;
        final Rectangle rectangle = SphericalMercatorUtils.recToSphericalMercator(GeoTileUtils.toBoundingBox(x, y, z));
        pointXScale = (double) extent / (rectangle.getMaxLon() - rectangle.getMinLon());
        pointYScale = (double) -extent / (rectangle.getMaxLat() - rectangle.getMinLat());
        pointXTranslate = -pointXScale * rectangle.getMinX();
        pointYTranslate = -pointYScale * rectangle.getMinY();
    }

    /**
     * Returns a {@code byte[]} containing the mvt representation of the provided point
     */
    public byte[] point(double lon, double lat) throws IOException {
        final int posLon = lon(lon);
        if (posLon > extent || posLon < 0) {
            return EMPTY;
        }
        final int posLat = lat(lat);
        if (posLat > extent || posLat < 0) {
            return EMPTY;
        }
        return point(posLon, posLat);
    }

    /**
     * Returns a {@code byte[]} containing the mvt representation of the provided points
     */
    public byte[] points(List<GeoPoint> multiPoint) {
        multiPoint.sort(Comparator.comparingDouble(GeoPoint::getLon).thenComparingDouble(GeoPoint::getLat));
        final int[] commands = new int[2 * multiPoint.size() + 1];
        int pos = 1, prevLon = 0, prevLat = 0, numPoints = 0;
        for (GeoPoint point : multiPoint) {
            final int posLon = lon(point.getLon());
            if (posLon > extent || posLon < 0) {
                continue;
            }
            final int posLat = lat(point.getLat());
            if (posLat > extent || posLat < 0) {
                continue;
            }
            // filter out repeated points
            if (numPoints == 0 || posLon != prevLon || posLat != prevLat) {
                commands[pos++] = BitUtil.zigZagEncode(posLon - prevLon);
                commands[pos++] = BitUtil.zigZagEncode(posLat - prevLat);
                prevLon = posLon;
                prevLat = posLat;
                numPoints++;
            }
        }
        if (numPoints == 0) {
            return EMPTY;
        }
        commands[0] = encodeCommand(MOVETO, numPoints);
        try {
            return writeCommands(commands, 1, pos);
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    /**
     * Returns a {@code byte[]} containing the mvt representation of the provided rectangle
     */
    public byte[] box(double minLon, double maxLon, double minLat, double maxLat) throws IOException {
        final int minX = Math.max(0, lon(minLon));
        if (minX > extent) {
            return EMPTY;
        }
        final int minY = Math.min(extent, lat(minLat));
        if (minY > extent) {
            return EMPTY;
        }
        final int maxX = Math.min(extent, lon(maxLon));
        if (maxX < 0) {
            return EMPTY;
        }
        final int maxY = Math.max(0, lat(maxLat));
        if (maxY < 0) {
            return EMPTY;
        }
        if (minX == maxX) {
            if (minY == maxY) {
                return point(minX, minY);
            } else {
                return line(minX, minY, minX, maxY);
            }
        } else if (minY == maxY) {
            return line(minX, minY, maxX, minY);
        } else {
            return box(minX, maxX, minY, maxY);
        }
    }

    private int lat(double lat) {
        return (int) Math.round(pointYScale * SphericalMercatorUtils.latToSphericalMercator(lat) + pointYTranslate) + extent;
    }

    private int lon(double lon) {
        return (int) Math.round(pointXScale * SphericalMercatorUtils.lonToSphericalMercator(lon) + pointXTranslate);
    }

    private static byte[] point(int x, int y) throws IOException {
        final int[] commands = new int[3];
        commands[0] = encodeCommand(MOVETO, 1);
        commands[1] = BitUtil.zigZagEncode(x);
        commands[2] = BitUtil.zigZagEncode(y);
        return writeCommands(commands, 1, 3);
    }

    private static byte[] line(int x1, int y1, int x2, int y2) throws IOException {
        final int[] commands = new int[6];
        commands[0] = encodeCommand(MOVETO, 1);
        commands[1] = BitUtil.zigZagEncode(x1);
        commands[2] = BitUtil.zigZagEncode(y1);

        commands[3] = encodeCommand(LINETO, 1);
        commands[4] = BitUtil.zigZagEncode(x2 - x1);
        commands[5] = BitUtil.zigZagEncode(y2 - y1);
        return writeCommands(commands, 2, 6);
    }

    private static byte[] box(int minX, int maxX, int minY, int maxY) throws IOException {
        final int[] commands = new int[11];
        commands[0] = encodeCommand(MOVETO, 1);
        commands[1] = BitUtil.zigZagEncode(minX);
        commands[2] = BitUtil.zigZagEncode(minY);
        commands[3] = encodeCommand(LINETO, 3);
        // 1
        commands[4] = BitUtil.zigZagEncode(0);
        commands[5] = BitUtil.zigZagEncode(maxY - minY);
        // 2
        commands[6] = BitUtil.zigZagEncode(maxX - minX);
        commands[7] = BitUtil.zigZagEncode(0);
        // 3
        commands[8] = BitUtil.zigZagEncode(0);
        commands[9] = BitUtil.zigZagEncode(minY - maxY);
        // close
        commands[10] = encodeCommand(CLOSEPATH, 1);
        return writeCommands(commands, 3, 11);
    }

    private static int encodeCommand(int id, int length) {
        return (id & 0x7) | (length << 3);
    }

    private static byte[] writeCommands(final int[] commands, final int type, final int length) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            for (int i = 0; i < length; i++) {
                output.writeVInt(commands[i]);
            }
            final int dataSize = output.size();
            output.reset();
            output.writeVInt(24);
            output.writeVInt(type);
            output.writeVInt(34);
            output.writeVInt(dataSize);
            for (int i = 0; i < length; i++) {
                output.writeVInt(commands[i]);
            }
            return output.copyBytes().array();
        }
    }
}
