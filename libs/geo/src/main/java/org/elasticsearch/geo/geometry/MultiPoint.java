/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.geo.geometry;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.elasticsearch.geo.GeoUtils;
import org.elasticsearch.geo.parsers.WKBParser;
import org.elasticsearch.geo.parsers.WKTParser;
import org.apache.lucene.store.OutputStreamDataOutput;

/**
 * Represents a MultiPoint object on the earth's surface in decimal degrees.
 */
public class MultiPoint extends GeoShape implements Iterable<Point> {
    protected final double[] lats;
    protected final double[] lons;

    public MultiPoint(double[] lats, double[] lons) {
        checkLatArgs(lats);
        checkLonArgs(lons);
        if (lats.length != lons.length) {
            throw new IllegalArgumentException("lats and lons must be equal length");
        }
        for (int i = 0; i < lats.length; i++) {
            GeoUtils.checkLatitude(lats[i]);
            GeoUtils.checkLongitude(lons[i]);
        }
        this.lats = lats.clone();
        this.lons = lons.clone();

        // compute bounding box
        double minLat = Math.min(lats[0], lats[lats.length - 1]);
        double maxLat = Math.max(lats[0], lats[lats.length - 1]);
        double minLon = Math.min(lons[0], lons[lats.length - 1]);
        double maxLon = Math.max(lons[0], lons[lats.length - 1]);

        double windingSum = 0d;
        final int numPts = lats.length - 1;
        for (int i = 1, j = 0; i < numPts; j = i++) {
            minLat = Math.min(lats[i], minLat);
            maxLat = Math.max(lats[i], maxLat);
            minLon = Math.min(lons[i], minLon);
            maxLon = Math.max(lons[i], maxLon);
            // compute signed area for orientation
            windingSum += (lons[j] - lons[numPts]) * (lats[i] - lats[numPts])
                    - (lats[j] - lats[numPts]) * (lons[i] - lons[numPts]);
        }
        this.boundingBox = new Rectangle(minLat, maxLat, minLon, maxLon);
    }

    @Override
    public ShapeType type() {
        return ShapeType.MULTIPOINT;
    }

    protected void checkLatArgs(double[] lats) {
        if (lats == null) {
            throw new IllegalArgumentException("lats must not be null");
        }
        if (lats.length < 2) {
            throw new IllegalArgumentException("at least 2 points are required");
        }
    }

    protected void checkLonArgs(double[] lons) {
        if (lons == null) {
            throw new IllegalArgumentException("lons must not be null");
        }
    }

    public int numPoints() {
        return lats.length;
    }

    private void checkVertexIndex(final int i) {
        if (i >= lats.length) {
            throw new IllegalArgumentException("Index " + i + " is outside the bounds of the " + lats.length + " vertices ");
        }
    }

    public double getLat(int vertex) {
        checkVertexIndex(vertex);
        return lats[vertex];
    }

    public double getLon(int vertex) {
        checkVertexIndex(vertex);
        return lons[vertex];
    }

    /**
     * Returns a copy of the internal latitude array
     */
    public double[] getLats() {
        return lats.clone();
    }

    /**
     * Returns a copy of the internal longitude array
     */
    public double[] getLons() {
        return lons.clone();
    }

    public Relation relate(double minLat, double maxLat, double minLon, double maxLon) {
        // note: this relate is not used; points are indexed as separate POINT types
        // note: if needed, we could build an in-memory BKD for each MultiPoint type
        throw new UnsupportedOperationException("use Point.relate instead");
    }

    public Relation relate(GeoShape shape) {
        return null;
    }

    @Override
    public boolean hasArea() {
        return false;
    }

    @Override
    public boolean equals(Object other) {
        if (super.equals(other) == false) return false;
        MultiPoint o = getClass().cast(other);
        return Arrays.equals(lats, o.lats) && Arrays.equals(lons, o.lons);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Arrays.hashCode(lats);
        result = 31 * result + Arrays.hashCode(lons);
        return result;
    }

    @Override
    protected StringBuilder contentToWKT() {
        return coordinatesToWKT(lats, lons);
    }

    protected static StringBuilder coordinatesToWKT(final double[] lats, final double[] lons) {
        StringBuilder sb = new StringBuilder();
        if (lats.length == 0) {
            sb.append(WKTParser.EMPTY);
        } else {
            // walk through coordinates:
            sb.append(WKTParser.LPAREN);
            sb.append(Point.coordinateToWKT(lats[0], lons[0]));
            for (int i = 1; i < lats.length; ++i) {
                sb.append(WKTParser.COMMA);
                sb.append(WKTParser.SPACE);
                sb.append(Point.coordinateToWKT(lats[i], lons[i]));
            }
            sb.append(WKTParser.RPAREN);
        }

        return sb;
    }

    @Override
    protected void appendWKBContent(OutputStreamDataOutput out) throws IOException {
        out.writeVInt(numPoints());
        pointsToWKB(lats, lons, out, true);
    }

    protected static OutputStreamDataOutput pointsToWKB(final double[] lats, final double[] lons,
                                                        OutputStreamDataOutput out, boolean writeHeader) throws IOException {
        final int numPoints = lats.length;
        for (int i = 0; i < numPoints; ++i) {
            if (writeHeader == true) {
                // write header for each coordinate (req. as part of spec for MultiPoints but not LineStrings)
                out.writeVInt(WKBParser.ByteOrder.XDR.ordinal());
                out.writeVInt(ShapeType.POINT.wkbOrdinal());
            }
            // write coordinates
            Point.coordinateToWKB(lats[i], lons[i], out);
        }
        return out;
    }

    @Override
    public Iterator<Point> iterator() {
        return new Iterator<Point>() {
            int i = 0;

            @Override
            public boolean hasNext() {
                return i < numPoints();
            }

            @Override
            public Point next() {
                return new Point(lats[i], lons[i]);
            }
        };
    }
}
