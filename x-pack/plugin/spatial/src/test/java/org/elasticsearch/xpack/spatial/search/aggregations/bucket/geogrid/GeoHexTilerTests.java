/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.Tessellator;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.xpack.spatial.geom.TestGeometryCollector;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.query.H3LatLonGeometry;
import org.elasticsearch.xpack.spatial.index.query.H3PolygonScaleRecommender;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.LATITUDE_MASK;
import static org.elasticsearch.xpack.spatial.common.Spatial3DUtils.calculateCentroid;
import static org.elasticsearch.xpack.spatial.common.Spatial3DUtils.pointInterpolation;
import static org.elasticsearch.xpack.spatial.util.GeoTestUtils.geoShapeValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public class GeoHexTilerTests extends GeoGridTilerTestCase {
    private final TestGeometryCollector testGeometryCollector = TestGeometryCollector.createWKTExporter("GeoHexTilerTests");

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

    /** The H3 tilers does not produce rectangular tiles, and some tests assume this */
    @Override
    protected boolean isRectangularTiler() {
        return false;
    }

    @Override
    protected long getCellsForDiffPrecision(int precisionDiff) {
        return UnboundedGeoHexGridTiler.calcMaxAddresses(precisionDiff);
    }

    public void testExtremeCells() throws IOException {
        for (String cell : new String[] { "8001fffffffffff", "80effffffffffff", "80f1fffffffffff" }) {
            Rectangle tile = new Rectangle(-180, 180, 85, -85);
            Rectangle shapeRectangle = new Rectangle(-180, 180, 85, -85);
            GeoShapeValues.GeoShapeValue value = geoShapeValue(shapeRectangle);

            GeoBoundingBox boundingBox = new GeoBoundingBox(
                new GeoPoint(tile.getMaxLat(), tile.getMinLon()),
                new GeoPoint(tile.getMinLat(), tile.getMaxLon())
            );
            H3PolygonScaleRecommender.Inflation inflation = H3PolygonScaleRecommender.PLANAR.recommend(cell);
            if (inflation.canInflate()) {
                boolean intersects = intersects(cell, value, boundingBox, inflation.scaleFactor());
                assertTrue(cell, intersects);
            } else {
                boolean intersects = intersects(cell, value, boundingBox, 1.0);
                assertTrue(cell, intersects);
            }
        }
    }

    public void testLargeBounds() throws Exception {
        // We have a shape and a tile both covering all mercator space, so we expect all level0 H3 cells to match
        testGeometryCollector.start("testLargeBounds");
        TestGeometryCollector.Collector collector = testGeometryCollector.normal();
        Rectangle tile = new Rectangle(-180, 180, 85, -85);
        collector.addBox(tile);
        Rectangle shapeRectangle = new Rectangle(-180, 180, 85, -85);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(shapeRectangle);

        GeoBoundingBox boundingBox = new GeoBoundingBox(
            new GeoPoint(tile.getMaxLat(), tile.getMinLon()),
            new GeoPoint(tile.getMinLat(), tile.getMaxLon())
        );

        GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), getBoundedGridTiler(boundingBox, 0), NOOP_BREAKER);
        assertTrue(values.advanceExact(0));
        int numTiles = values.docValueCount();
        int expectedTiles = expectedBuckets(value, 0, boundingBox);
        for (int i = 0; i < numTiles; i++) {
            long h3 = values.getValues()[i];
            collector.addH3Cell(H3.h3ToString(h3));
        }
        testGeometryCollector.stop((normal, special) -> {
            assertThat(normal.size(), is(expectedTiles + 1));
            // assertThat(normal.size(), is(122 + 1)); // All 122 H3 cells plus the original rectangle
        });
        assertThat(expectedTiles, equalTo(numTiles));
    }

    public void testTilerBruteForceMethods() {
        // TODO: This test currently just measures the difference between the cells expected and the cells found and can be removed
        int baseHexagons = 110;
        int basePentagons = 12;
        String find = "81033ffffffffff";
        // System.out.printf("%10s\t%10s\t%10s\t%10s\n", "Precision", "cells", "expected", "diff");
        for (int precision = 0; precision < 6; precision++) {
            UnboundedGeoHexGridTiler tiler = new UnboundedGeoHexGridTiler(precision);
            List<String> cells = tiler.getAllCellsAt(precision);
            List<String> found = cells.stream().filter(v -> v.equals(find)).toList();
            int count = cells.size();
            long expectedCount = baseHexagons * (long) Math.pow(7, precision) + basePentagons * (long) Math.pow(6, precision);
            // assertThat("Number of cells at precision " + precision, count, equalTo(expectedCount));
            long diff = count < 0 ? count : count - expectedCount;
            long factor = diff < 0 ? -1 : diff / 12;
            // System.out.printf("%10d\t%10d\t%10d\t%10d%10d\t%s\n", precision, count, expectedCount, diff, factor, found);
        }
    }

    public void testGeoGridSetValuesBruteAndRecursiveMultiPoint_Specific() throws Exception {
        // TODO: Remove this test once the special case it relates to is fixed, because this is covered by other tests
        // MULTIPOINT (0.0 -65.05010865513388, -163.4905660431598 1.401298464324817E-45, 0.0 81.59665381977166, -161.94604140338802
        // 46.93321798968367)
        Point[] points = new Point[] {
            new Point(0.0, -65.05010865513388),
            new Point(-163.4905660431598, 1.401298464324817E-45),
            new Point(0.0, 81.59665381977166),
            new Point(-161.94604140338802, 46.93321798968367) };
        Geometry geometry = new MultiPoint(Arrays.asList(points));
        int precision = 1;

        HashSet<String> expectedCells = new HashSet<>();
        for (Point point : points) {
            expectedCells.add(H3.geoToH3Address(point.getLat(), point.getLon(), precision));
        }

        testGeometryCollector.start(makeNameFromStack());
        TestGeometryCollector.Collector collector = testGeometryCollector.normal();
        collector.addGeometry(geometry);
        UnboundedGeoHexGridTiler tiler = new UnboundedGeoHexGridTiler(precision);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);

        GeoShapeCellValues recursiveValues = new GeoShapeCellValues(null, tiler, NOOP_BREAKER);
        int recursiveCount = tiler.setValuesByRecursion(recursiveValues, value);
        findMissing(expectedCells, recursiveValues);

        GeoShapeCellValues bruteForceValues = new GeoShapeCellValues(null, tiler, NOOP_BREAKER);
        int bruteForceCount = tiler.setValuesByBruteForce(bruteForceValues, value);
        findMissing(expectedCells, recursiveValues);

        addResultsToCollector(collector, recursiveValues);
        testGeometryCollector.stop();

        assertThat(geometry.toString(), recursiveCount, equalTo(bruteForceCount));

        long[] recursive = Arrays.copyOf(recursiveValues.getValues(), recursiveCount);
        long[] bruteForce = Arrays.copyOf(bruteForceValues.getValues(), bruteForceCount);
        Arrays.sort(recursive);
        Arrays.sort(bruteForce);
        assertArrayEquals(geometry.toString(), recursive, bruteForce);
    }

    private Set<String> findMissing(Collection<String> expected, GeoShapeCellValues cellValues) {
        HashSet<String> found = new HashSet<>();
        HashSet<String> missing = new HashSet<>();
        long[] values = cellValues.getValues();
        for (int i = 0; i < cellValues.docValueCount(); i++) {
            found.add(H3.h3ToString(values[i]));
        }
        for (String address : expected) {
            if (found.contains(address) == false) {
                missing.add(address);
            }
        }
        return missing;
    }

    public void testGeoGridSetValuesBoundingBoxes_UnboundedGeoShapeCellValues_Specific() throws Exception {
        // TODO: Remove this test once the special case it relates to is fixed, because this is covered by other tests
        // Specifically GeoGridTilerTestCase.testGeoGridSetValuesBoundingBoxes_UnboundedGeoShapeCellValues
        testGeometryCollector.start("testGeoGridSetValuesBoundingBoxes_UnboundedGeoShapeCellValues_Specific");
        TestGeometryCollector.Collector collector = testGeometryCollector.normal();
        // polygon=linearring(x=[-75.38472703981472, 101.98625624670547, 101.98625624670547, -75.38472703981472, -75.38472703981472],
        // y=[24.849476721220427, 24.849476721220427, 36.4396446870375, 36.4396446870375, 24.849476721220427])
        // double[] lons = new double[] {-75.38472703981472,101.98625624670547,101.98625624670547,-75.38472703981472,-75.38472703981472};
        // double[] lats = new double[] { 24.849476721220427, 24.849476721220427, 36.4396446870375, 36.4396446870375, 24.849476721220427 };
        // double x = 80;
        // double y = 36;
        // double[] lons = new double[] { -x, x, x, -x, -x };
        // double[] lats = new double[] { 25, 25, y, y, 25 };
        // double[] lons = new double[] {-75.38472703981472,101.98625624670547,101.98625624670547,-75.38472703981472,-75.38472703981472};
        // double[] lats = new double[] { 24.849476721220427, 24.849476721220427, 36.4396446870375, 36.4396446870375, 24.849476721220427 };
        double[] lons = new double[] { -75.4, 102, 102, -75.4, -75.4 };
        double[] lats = new double[] { 24.85, 24.85, 36.44, 36.44, 24.85 };
        LinearRing ring = new LinearRing(lons, lats);
        Polygon geometry = interpolate(new Polygon(ring), 1);
        addPolygon(collector, geometry, true);
        new WidthValidator().validate(geometry);
        addTriangles(collector, geometry);
        int precision = 2;

        GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);
        GeoShapeCellValues unboundedCellValues = new GeoShapeCellValues(
            makeGeoShapeValues(value),
            getUnboundedGridTiler(precision),
            NOOP_BREAKER
        );
        assertTrue(unboundedCellValues.advanceExact(0));
        addPolygons(collector, unboundedCellValues);
        testGeometryCollector.stop();
        int numBuckets = unboundedCellValues.docValueCount();
        int expected = expectedBuckets(value, precision, null);
        // System.out.println("For precision " + precision + " we got " + numBuckets + " buckets while expecting " + expected);
        // System.out.println(geometry);
        assertThat("[" + precision + "] bucket count", numBuckets, equalTo(expected));
    }

    public void testGeoGridSetValuesBoundingBoxes_UnboundedGeoShapeCellValues_Specific2() throws Exception {
        // TODO: Remove this test once the special case it relates to is fixed, because this is covered by other tests
        // Specifically GeoGridTilerTestCase.testGeoGridSetValuesBoundingBoxes_UnboundedGeoShapeCellValues
        // polygon=linearring(x=[0.9999999403953552, 180.0, 180.0, 0.9999999403953552, 0.9999999403953552], y=[5.1678237312901985,
        // 5.1678237312901985, 90.0, 90.0, 5.1678237312901985])
        testGeometryCollector.start("testGeoGridSetValuesBoundingBoxes_UnboundedGeoShapeCellValues_Specific2", +50, 0);
        TestGeometryCollector.Collector collector = testGeometryCollector.normal();
        int precision = 1;
        // double[] lons = new double[] { 0.9999999403953552, 180.0, 180.0, 0.9999999403953552, 0.9999999403953552 };
        // double[] lats = new double[] { 5.1678237312901985, 5.1678237312901985, 70.0, 70.0, 5.1678237312901985 };
        for (int i = 0; i < 10; i++) {
            double left = 10d - i;
            double[] lons = new double[] { left, 180.0, 180.0, left, left };
            double[] lats = new double[] { 10, 10, 70.0, 70.0, 10 };
            LinearRing ring = new LinearRing(lons, lats);
            Polygon geometry = new Polygon(ring);
            addPolygon(collector, geometry, true);
            new WidthValidator().validate(geometry);
            addTriangles(collector, geometry);

            GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);
            GeoShapeCellValues unboundedCellValues = new GeoShapeCellValues(
                makeGeoShapeValues(value),
                getUnboundedGridTiler(precision),
                NOOP_BREAKER
            );
            assertTrue(unboundedCellValues.advanceExact(0));
            addPolygons(collector, unboundedCellValues);
            int numBuckets = unboundedCellValues.docValueCount();
            List<String> expected = expectedBucketsX(value, precision, null);
            // System.out.println("For left=" + left + " we got " + numBuckets + " buckets while expecting " + expected.size());
            // System.out.println(geometry);
            if (expected.size() != numBuckets) {
                long[] values = unboundedCellValues.getValues();
                HashSet<String> expectedButNotFound = new HashSet<>();
                HashSet<String> foundButNotExpected = new HashSet<>();
                for (int j = 0; j < numBuckets; j++) {
                    long bucket = values[j];
                    String address = H3.h3ToString(bucket);
                    if (expected.contains(address) == false) {
                        collector.addH3Cell(address);
                        collector.addH3Cell(address);
                        collector.addH3Cell(address);
                        foundButNotExpected.add(address);
                    }
                }
                for (String address : expected) {
                    long h3 = H3.stringToH3(address);
                    boolean found = false;
                    for (int j = 0; j < numBuckets; j++) {
                        if (values[j] == h3) {
                            found = true;
                            break;
                        }
                    }
                    if (found == false) {
                        collector.addH3Cell(address);
                        collector.addH3Cell(address);
                        collector.addH3Cell(address);
                        expectedButNotFound.add(address);
                    }
                }
                // System.out.println("Expected but not found [" + expectedButNotFound.size() + "]: " + expectedButNotFound);
                // System.out.println("Found but not expected [" + foundButNotExpected.size() + "]: " + foundButNotExpected);
            }
            testGeometryCollector.stop();
            assertThat("[" + left + "] bucket count", numBuckets, equalTo(expected.size()));
        }
    }

    Polygon interpolate(Polygon polygon, int multiplier) {
        assertThat(multiplier, greaterThan(0));
        assertThat(multiplier, lessThan(1000));
        double[] lons = polygon.getPolygon().getLons();
        double[] lats = polygon.getPolygon().getLats();
        double[] newLons = new double[multiplier * (lons.length - 1) + 1];
        double[] newLats = new double[multiplier * (lats.length - 1) + 1];
        for (int i = 0; i < lons.length; i++) {
            if (i == lons.length - 1) {
                newLons[i * multiplier] = lons[i];
                newLats[i * multiplier] = lats[i];
            } else {
                for (int m = 0; m < multiplier; m++) {
                    double lon = (lons[i] * (multiplier - m) + lons[i + 1] * m) / multiplier;
                    double lat = (lats[i] * (multiplier - m) + lats[i + 1] * m) / multiplier;
                    newLons[i * multiplier + m] = lon;
                    newLats[i * multiplier + m] = lat;
                }
            }
        }
        return new Polygon(new LinearRing(newLons, newLats));
    }

    @Override
    protected void assertSetValuesBruteAndRecursive(Geometry geometry, int precision) throws Exception {
        testGeometryCollector.start(makeNameFromStack());
        TestGeometryCollector.Collector collector = testGeometryCollector.normal();
        collector.addGeometry(geometry);
        UnboundedGeoHexGridTiler tiler = new UnboundedGeoHexGridTiler(precision);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);

        GeoShapeCellValues recursiveValues = new GeoShapeCellValues(null, tiler, NOOP_BREAKER);
        int recursiveCount = tiler.setValuesByRecursion(recursiveValues, value);

        GeoShapeCellValues bruteForceValues = new GeoShapeCellValues(null, tiler, NOOP_BREAKER);
        int bruteForceCount = tiler.setValuesByBruteForce(bruteForceValues, value);

        addResultsToCollector(collector, recursiveValues);
        addResultsToCollector(collector, bruteForceValues);
        testGeometryCollector.stop();

        assertThat(geometry.toString(), recursiveCount, equalTo(bruteForceCount));

        long[] recursive = Arrays.copyOf(recursiveValues.getValues(), recursiveCount);
        long[] bruteForce = Arrays.copyOf(bruteForceValues.getValues(), bruteForceCount);
        Arrays.sort(recursive);
        Arrays.sort(bruteForce);
        assertArrayEquals(geometry.toString(), recursive, bruteForce);
    }

    private void addResultsToCollector(TestGeometryCollector.Collector collector, GeoShapeCellValues values) {
        long[] h3Values = values.getValues();
        for (int i = 0; i < values.docValueCount(); i++) {
            String address = H3.h3ToString(h3Values[i]);
            collector.addH3Cell(address);
        }
    }

    private String makeNameFromStack() {
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        for (StackTraceElement element : stack) {
            String methodName = element.getMethodName();
            if (methodName.startsWith("test")) {
                return methodName;
            }
        }
        throw new IllegalStateException("Could not find a test method in the stack");
    }

    protected boolean geometryIsInvalid(Geometry g) {
        return super.geometryIsInvalid(g) || geometryTooWide(g);
    }

    private boolean geometryTooWide(Geometry g) {
        try {
            new WidthValidator().validate(g);
            return false;
        } catch (Exception e) {
            return true;
        }
    }

    public static class WidthValidator extends GeographyValidator {
        double minLon = Float.MAX_VALUE;
        double maxLon = -Float.MAX_VALUE;

        public WidthValidator() {
            super(true);
        }

        @Override
        protected void checkLongitude(double longitude) {
            super.checkLongitude(longitude);
            if (longitude < minLon) {
                minLon = longitude;
            }
            if (longitude > maxLon) {
                maxLon = longitude;
            }
            if (width() >= 180) {
                throw new IllegalArgumentException("invalid longitude " + longitude + "; results in width " + width());
            }
        }

        double width() {
            return maxLon - minLon;
        }
    }

    @Override
    protected int expectedBuckets(GeoShapeValues.GeoShapeValue geoValue, int precision, GeoBoundingBox bbox) throws Exception {
        GeoShapeValues.BoundingBox bounds = geoValue.boundingBox();
        if (bounds.minX() == bounds.maxX() && bounds.minY() == bounds.maxY()) {
            String address = H3.geoToH3Address(bounds.minX(), bounds.minY(), precision);
            if (intersects(address, geoValue, bbox, 1.0)) {
                return 1;
            }
            return 0;
        }
        return computeBuckets(H3.getStringRes0Cells(), bbox, geoValue, precision);
    }

    private int computeBuckets(String[] children, GeoBoundingBox bbox, GeoShapeValues.GeoShapeValue geoValue, int finalPrecision)
        throws IOException {
        int count = 0;
        ArrayList<String> missing = new ArrayList<>();
        for (String child : children) {
            if (H3.getResolution(child) == finalPrecision) {
                if (intersects(child, geoValue, bbox, 1.0)) {
                    count++;
                } else {
                    missing.add(child);
                }
            } else {
                H3PolygonScaleRecommender.Inflation inflation = H3PolygonScaleRecommender.PLANAR.recommend(child);
                if (inflation.canInflate()) {
                    if (intersects(child, geoValue, bbox, inflation.scaleFactor())) {
                        count += computeBuckets(H3.h3ToChildren(child), bbox, geoValue, finalPrecision);
                    }
                } else {
                    if (intersects(child, geoValue, bbox, 1.0)) {
                        count += computeBuckets(H3.h3ToChildren(child), bbox, geoValue, finalPrecision);
                    }
                    for (String neighbour : H3.hexRing(child)) {
                        if (intersects(neighbour, geoValue, bbox, 1.0)) {
                            count += computeBuckets(H3.h3ToChildren(neighbour), bbox, geoValue, finalPrecision);
                        }
                    }
                }
            }
        }
        if (missing.size() > 0) {
            System.out.println("We were missing " + missing.size() + " cells:");
            for (String cell : missing) {
                System.out.println("\t" + cell);
            }
        }
        return count;
    }

    // TODO this more expensive version can be removed once we are sure all testing is comprehensive
    protected List<String> expectedBucketsX(GeoShapeValues.GeoShapeValue geoValue, int precision, GeoBoundingBox bbox) throws Exception {
        GeoShapeValues.BoundingBox bounds = geoValue.boundingBox();
        if (bounds.minX() == bounds.maxX() && bounds.minY() == bounds.maxY()) {
            String address = H3.geoToH3Address(bounds.minX(), bounds.minY(), precision);
            if (intersects(address, geoValue, bbox, 1.0)) {
                return Collections.singletonList(address);
            }
            return Collections.emptyList();
        }
        return computeBucketsX(H3.getStringRes0Cells(), bbox, geoValue, precision);
    }

    private List<String> computeBucketsX(String[] children, GeoBoundingBox bbox, GeoShapeValues.GeoShapeValue geoValue, int finalPrecision)
        throws IOException {
        ArrayList<String> count = new ArrayList<>();
        for (String child : children) {
            if (H3.getResolution(child) == finalPrecision) {
                if (intersects(child, geoValue, bbox, 1.0)) {
                    count.add(child);
                }
            } else {
                H3PolygonScaleRecommender.Inflation inflation = H3PolygonScaleRecommender.PLANAR.recommend(child);
                if (inflation.canInflate()) {
                    if (intersects(child, geoValue, bbox, inflation.scaleFactor())) {
                        count.addAll(computeBucketsX(H3.h3ToChildren(child), bbox, geoValue, finalPrecision));
                    }
                } else {
                    if (intersects(child, geoValue, bbox, 1.0)) {
                        count.addAll(computeBucketsX(H3.h3ToChildren(child), bbox, geoValue, finalPrecision));
                    }
                    for (String neighbour : H3.hexRing(child)) {
                        if (intersects(neighbour, geoValue, bbox, 1.0)) {
                            count.addAll(computeBucketsX(H3.h3ToChildren(neighbour), bbox, geoValue, finalPrecision));
                        }
                    }
                }
            }
        }
        return count;
    }

    private boolean intersects(String address, GeoShapeValues.GeoShapeValue geoValue, GeoBoundingBox bbox, double inflationFactor)
        throws IOException {
        if (addressIntersectsBounds(address, bbox, inflationFactor) == false) {
            return false;
        }
        final H3LatLonGeometry geometry = new GeoHexBoundedPredicate.H3LatLonGeom.Scaled(address, inflationFactor);
        return geoValue.relate(geometry) != GeoRelation.QUERY_DISJOINT;
    }

    private boolean addressIntersectsBounds(String address, GeoBoundingBox bbox, double inflationFactor) {
        if (bbox == null) {
            return true;
        }
        GeoHexBoundedPredicate predicate = new GeoHexBoundedPredicate(address.length(), bbox);
        return predicate.validAddress(address, inflationFactor);
    }

    public void testGeoGridSetValuesBruteAndRecursiveSpecificPoint() throws Exception {
        // Geometry geometry = new Point(-128.86426365551742, 90.0);
        Geometry geometry = new Point(0.0, 24.10728934939077);
        assertSetValuesBruteAndRecursive(geometry, 1);
    }

    private void expect(Map<Double, Map<Integer, Integer>> expected, double scaleFactor, int precision, int count) {
        expected.computeIfAbsent(scaleFactor, k -> new HashMap<>());
        expected.get(scaleFactor).put(precision, count);
    }

    public void testTroublesomeH3Cell() throws Exception {
        testGeometryCollector.start("testTroublesomeH3Cell");
        TestGeometryCollector.Collector collector = testGeometryCollector.normal();
        double x = 146.58268694992358;
        double y = 68.64804543269979;
        double scaleFactor = 0.8;
        int precision = 5;
        int expected = 1;
        long h3 = H3.geoToH3(y, x, 5);
        CellBoundary cellBoundary = H3.h3ToGeoBoundary(h3);
        Polygon polygon = makeScaledH3Cell(cellBoundary, 0.8);
        collector.addH3Cell(H3.h3ToString(h3));
        addPolygon(collector, polygon, true);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(polygon);
        String description = String.format(Locale.ROOT, "Polygon scaled %f, precision %d expects %d", scaleFactor, precision, expected);
        GeoShapeCellValues values = new GeoShapeCellValues(
            makeGeoShapeValues(value),
            new UnboundedGeoHexGridTiler(precision),
            NOOP_BREAKER
        );
        assertTrue(description, values.advanceExact(0));
        int count = values.docValueCount();
        addPolygons(collector, values);
        testGeometryCollector.stop();
        assertThat(description, count, equalTo(expected));
    }

    public void testGeoHex() throws Exception {
        // -Dtests.seed=9D0AE5E354F41D62 Fails on level 5
        testGeometryCollector.start("testGeoHex");
        TestGeometryCollector.Collector collector = testGeometryCollector.normal();
        double x = randomDoubleBetween(-180, 180, true);
        double y = randomDoubleBetween(-LATITUDE_MASK, LATITUDE_MASK, false);
        int level = randomIntBetween(0, 6);
        assertThat(new UnboundedGeoHexGridTiler(level).encode(x, y), equalTo(H3.geoToH3(y, x, level)));

        Map<Double, Map<Integer, Integer>> expected = new LinkedHashMap<>();
        expect(expected, 0.8, 5, 1); // original cell only
        expect(expected, 0.8, 6, 1 + 6); // center child and six other children
        expect(expected, 0.8, 7, 1 + 6 + 6 * 6); // 6/7 of all outer children
        expect(expected, 0.9, 5, 1); // original cell only
        expect(expected, 0.9, 6, 1 + 6 + 6); // center child and six other children, and 6 neighbouring children
        expect(expected, 0.9, 7, 1 + 6 + 6 * 7 + 6); // all 7 outer children, and 6 more from neighbours
        expect(expected, 1.1, 5, 1 + 6); // original cell and six neighbours
        expect(expected, 1.1, 6, 1 + 6 + 6 * 2); // center child and six other children, and 12 neighbouring children
        expect(expected, 1.1, 7, 1 + 6 + 6 * 7 + 6 * 4); // all 7 outer children, and 24 more from neighbours
        expect(expected, 1.2, 5, 1 + 6); // original cell and six neighbours
        expect(expected, 1.2, 6, 1 + 6 + 6 * 2); // center child and six other children, and 12 neighbouring children
        expect(expected, 1.2, 7, 1 + 6 + 6 * 7 + 6 * 7); // all 7 outer children, and 42 more from neighbours

        // Create a polygon scaled from a single H3 cell at precision 5
        long h3 = H3.geoToH3(y, x, 5);
        CellBoundary cellBoundary = H3.h3ToGeoBoundary(h3);
        collector.addH3Cell(H3.h3ToString(h3));
        for (double scaleFactor : expected.keySet()) {
            Map<Integer, Integer> counts = expected.get(scaleFactor);
            Polygon polygon = makeScaledH3Cell(cellBoundary, scaleFactor);
            collector.addPolygon(polygon);
            GeoShapeValues.GeoShapeValue value = geoShapeValue(polygon);

            for (int precision = 5; precision < 8; precision++) {
                String description = String.format(
                    Locale.ROOT,
                    "Polygon scaled %f, precision %d expects %d",
                    scaleFactor,
                    precision,
                    counts.get(precision)
                );
                GeoShapeCellValues values = new GeoShapeCellValues(
                    makeGeoShapeValues(value),
                    new UnboundedGeoHexGridTiler(precision),
                    NOOP_BREAKER
                );
                assertTrue(description, values.advanceExact(0));
                int count = values.docValueCount();
                addPolygons(collector, values);
                assertThat(description, count, equalTo(counts.get(precision)));
            }
        }
        testGeometryCollector.stop();
    }

    private Polygon makeScaledH3Cell(CellBoundary cellBoundary, double scaleFactor) {
        Point centroid = calculateCentroid(cellBoundary);
        double[] lats = new double[cellBoundary.numPoints() + 1];
        double[] lons = new double[cellBoundary.numPoints() + 1];
        for (int i = 0; i < cellBoundary.numPoints(); i++) {
            LatLng latlng = cellBoundary.getLatLon(i);
            Point point = pointInterpolation(centroid, new Point(latlng.getLonDeg(), latlng.getLatDeg()), scaleFactor);
            lats[i] = point.getLat();
            lons[i] = point.getLon();
        }
        // Close the ring
        lats[lats.length - 1] = lats[0];
        lons[lats.length - 1] = lons[0];
        return new Polygon(new LinearRing(lons, lats));
    }

    private void addTriangles(TestGeometryCollector.Collector collector, Polygon polygon) {
        org.apache.lucene.geo.Polygon lucenePolygon = new org.apache.lucene.geo.Polygon(
            polygon.getPolygon().getY(),
            polygon.getPolygon().getX()
        );
        addTriangles(collector, Tessellator.tessellate(lucenePolygon, true));
    }

    private void addTriangles(TestGeometryCollector.Collector collector, List<Tessellator.Triangle> triangles) {
        ArrayList<Point> points = new ArrayList<>();
        for (Tessellator.Triangle triangle : triangles) {
            points.clear();
            for (int i = 0; i <= 3; i++) {
                points.add(new Point(triangle.getX(i % 3), triangle.getY(i % 3)));
            }
            collector.addPolygon(points);
        }
    }

    private void addPolygons(TestGeometryCollector.Collector collector, GeoShapeCellValues values) {
        long[] h3values = values.getValues();
        for (int i = 0; i < values.docValueCount(); i++) {
            long h3 = h3values[i];
            collector.addH3Cell(H3.h3ToString(h3));
        }
    }

    private void addPolygon(TestGeometryCollector.Collector collector, Polygon polygon, boolean addPoints) {
        collector.addPolygon(polygon);
        if (addPoints) {
            LinearRing ring = polygon.getPolygon();
            for (int i = 0; i < ring.length(); i++) {
                collector.addPoint(ring.getX(i), ring.getY(i));
            }
        }
    }
}
