/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.geo.Component2D;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.xpack.spatial.util.GeoTestUtils.geoShapeValue;
import static org.hamcrest.Matchers.equalTo;

public class GeoHexTilerTests extends GeoGridTilerTestCase {

    @Override
    protected GeoGridTiler getUnboundedGridTiler(int precision) {
        return new UnboundedGeoHexGridTiler(precision);
    }

    @Override
    protected GeoGridTiler getBoundedGridTiler(GeoBoundingBox bbox, int precision) {
        return new BoundedGeoHexGridTiler(precision, bbox);
    }

    @Override
    protected int maxPrecision() {
        return H3.MAX_H3_RES;
    }

    @Override
    protected Rectangle getCell(double lon, double lat, int precision) {
        Component2D component = new GeoHexBoundedPredicate.H3LatLonGeom(H3.geoToH3Address(lat, lon, precision)).toComponent2D();
        return new Rectangle(component.getMinX(), component.getMaxX(), component.getMaxY(), component.getMinY());
    }

    @Override
    protected long getCellsForDiffPrecision(int precisionDiff) {
        // TODO: Verify equation
        return 122L + (long) Math.pow(7, precisionDiff);
    }

    @Override
    protected void assertSetValuesBruteAndRecursive(Geometry geometry) throws Exception {
        int precision = randomIntBetween(1, 3);
        UnboundedGeoHexGridTiler tiler = new UnboundedGeoHexGridTiler(precision);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);
        GeoShapeCellValues recursiveValues = new GeoShapeCellValues(null, tiler, NOOP_BREAKER);
        int recursiveCount;
        {
            recursiveCount = tiler.setValuesByRecursion(recursiveValues, value);
        }
        GeoShapeCellValues bruteForceValues = new GeoShapeCellValues(null, tiler, NOOP_BREAKER);
        int bruteForceCount;
        {
            bruteForceCount = tiler.setValuesByBruteForce(bruteForceValues, value);
        }

        assertThat(geometry.toString(), recursiveCount, equalTo(bruteForceCount));

        long[] recursive = Arrays.copyOf(recursiveValues.getValues(), recursiveCount);
        long[] bruteForce = Arrays.copyOf(bruteForceValues.getValues(), bruteForceCount);
        Arrays.sort(recursive);
        Arrays.sort(bruteForce);
        assertArrayEquals(geometry.toString(), recursive, bruteForce);
    }

    @Override
    protected int expectedBuckets(GeoShapeValues.GeoShapeValue geoValue, int precision, GeoBoundingBox bbox) throws Exception {
        GeoShapeValues.BoundingBox bounds = geoValue.boundingBox();
        if (bounds.minX() == bounds.maxX() && bounds.minY() == bounds.maxY()) {
            String address = H3.geoToH3Address(bounds.minX(), bounds.minY(), precision);
            if (addressIntersectsBounds(address, bbox) && intersects(address, geoValue)) {
                return 1;
            }
            return 0;
        }
        return computeBuckets(H3.getStringRes0Cells(), bbox, geoValue, precision);
    }

    private int computeBuckets(String[] children, GeoBoundingBox bbox, GeoShapeValues.GeoShapeValue geoValue, int finalPrecision)
        throws IOException {
        int count = 0;
        for (String child : children) {
            if (addressIntersectsBounds(child, bbox) == false) {
                continue;
            }
            if (intersects(child, geoValue)) {
                if (H3.getResolution(child) == finalPrecision) {
                    count++;
                } else {
                    count += computeBuckets(H3.h3ToChildren(child), bbox, geoValue, finalPrecision);
                }
            }
        }
        return count;
    }

    private boolean intersects(String address, GeoShapeValues.GeoShapeValue geoValue) throws IOException {
        final GeoHexBoundedPredicate.H3LatLonGeom geometry = new GeoHexBoundedPredicate.H3LatLonGeom(address);
        return geoValue.relate(geometry) != GeoRelation.QUERY_DISJOINT;
    }

    private boolean addressIntersectsBounds(String address, GeoBoundingBox bbox) {
        if (bbox == null) {
            return true;
        }
        GeoHexBoundedPredicate predicate = new GeoHexBoundedPredicate(address.length(), bbox);
        return predicate.validAddress(address);
    }

    public void testGeoHex() throws Exception {
        double x = randomDoubleBetween(-180, 180, true);
        double y = randomDoubleBetween(-90, 90, false);
        int precision = randomIntBetween(0, 6);
        assertThat(new UnboundedGeoHexGridTiler(precision).encode(x, y), equalTo(H3.geoToH3(y, x, precision)));

        // Create a polygon slightly smaller than a single H3 cell at precision 5
        CellBoundary cellBoundary = H3.h3ToGeoBoundary(H3.geoToH3(y, x, 5));
        double[] lats = new double[cellBoundary.numPoints() + 1];
        double[] lons = new double[cellBoundary.numPoints() + 1];
        double lat = 0;
        double lng = 0;
        for (int i = 0; i < cellBoundary.numPoints(); i++) {
            LatLng point = cellBoundary.getLatLon(i);
            lats[i] = point.getLatDeg();
            lons[i] = point.getLonDeg();
            lat += lats[i];
            lng += lons[i];
        }
        // Close the ring
        lats[lats.length - 1] = lats[0];
        lons[lats.length - 1] = lons[0];
        lat /= cellBoundary.numPoints();
        lng /= cellBoundary.numPoints();
        // TODO: Remove debugging output
        StringBuilder sb = new StringBuilder("GEOMETRYCOLLECTION(");
        sb.append("POINT(").append(lng).append(" ").append(lat).append("),");
        addPolygon(sb, lons, lats);
        sb.append(",");
        double weight = 0.1; // move all points inwards by this fraction of the distance to the centroid
        for (int i = 0; i < lats.length; i++) {
            lats[i] = (1.0 - weight) * lats[i] + weight * lat;
            lons[i] = (1.0 - weight) * lons[i] + weight * lng;
        }
        Polygon polygon = new Polygon(new LinearRing(lons, lats));
        GeoShapeValues.GeoShapeValue value = geoShapeValue(polygon);
        addPolygon(sb, lons, lats);
        for (int i = 0; i < lats.length; i++) {
            sb.append(",POINT(");
            sb.append(lons[i]).append(" ").append(lats[i]);
            sb.append(")");
        }
        System.out.println(sb + ")");

        // test shape within tile bounds
        {
            GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), new UnboundedGeoHexGridTiler(5), NOOP_BREAKER);
            assertTrue(values.advanceExact(0));
            int count = values.docValueCount();
            addPolygons(5, sb, values);
            System.out.println(sb + ")");
            assertThat(count, equalTo(1));
        }
        {
            GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), new UnboundedGeoHexGridTiler(6), NOOP_BREAKER);
            assertTrue(values.advanceExact(0));
            int count = values.docValueCount();
            addPolygons(6, sb, values);
            System.out.println(sb + ")");
            //assertThat(count, equalTo(7));
        }
        {
            GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), new UnboundedGeoHexGridTiler(7), NOOP_BREAKER);
            assertTrue(values.advanceExact(0));
            int count = values.docValueCount();
            addPolygons(7, sb, values);
            System.out.println(sb + ")");
            assertThat(count, equalTo(7 * 7));
        }
    }

    private void addPolygons(int precision, StringBuilder sb, GeoShapeCellValues values) {
        long[] h3values = values.getValues();
        for (int i = 0; i < values.docValueCount(); i++) {
            long h3 = h3values[i];
            System.out.println("Adding polygon at depth " + precision + ": " + h3);
            sb.append(",");
            addPolygon(sb, H3.h3ToGeoBoundary(h3));
        }
    }

    private void addPolygon(StringBuilder sb, CellBoundary cell) {
        sb.append("POLYGON((");
        for (int i = 0; i <= cell.numPoints(); i++) {
            if (i == cell.numPoints()) {
                LatLng point = cell.getLatLon(0);
                sb.append(point.getLonDeg()).append(" ");
                sb.append(point.getLatDeg());
            } else {
                LatLng point = cell.getLatLon(i);
                sb.append(point.getLonDeg()).append(" ");
                sb.append(point.getLatDeg()).append(", ");
            }
        }
        sb.append("))");
    }

    private void addPolygon(StringBuilder sb, double[] lons, double[] lats) {
        sb.append("POLYGON((");
        for (int i = 0; i <= lats.length; i++) {
            if (i == lats.length) {
                sb.append(lons[0]).append(" ");
                sb.append(lats[0]);
            } else {
                sb.append(lons[i]).append(" ");
                sb.append(lats[i]).append(", ");
            }
        }
        sb.append("))");
    }

}
