/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.geo.Component2D;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.geom.TestGeometryCollector;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;

import static org.elasticsearch.xpack.spatial.common.Spatial3DUtils.calculateCentroid;
import static org.elasticsearch.xpack.spatial.common.Spatial3DUtils.pointInterpolation;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * These tests are run against both H3LatLonGeometry.Spherical and H3LatLonGeometry.Planar.
 */
public abstract class H3PolygonScaleRecommenderTests extends ESTestCase {

    private final TestGeometryCollector testGeometryCollector = TestGeometryCollector.createGeometryCollector();
    // Uncomment the following to enable export of WKT output for specific tests when debugging with -Dtests.security.manager=false
    // private final TestGeometryCollector testGeometryCollector = TestGeometryCollector.createWKTExporter(getTestClass().getSimpleName());

    protected abstract H3LatLonGeometry makeGeometry(String h3Address);

    protected abstract H3LatLonGeometry makeGeometry(String h3Address, double scaleFactor);

    protected abstract double getLatitudeThreshold();

    protected abstract H3PolygonScaleRecommender scaleRecommender();

    public void testInflatedContainsAllChildren() {
        for (int i = 0; i < 10; i++) {
            Point point = randomSafePoint();
            String cell = H3.geoToH3Address(point.getLat(), point.getLon(), 0);
            H3PolygonScaleRecommender.Inflation inflation = scaleRecommender().recommend(cell);
            if (inflation.canInflate()) {
                H3LatLonGeometry hexagon = makeGeometry(cell, inflation.scaleFactor());
                try {
                    Component2D component2D = hexagon.toComponent2D();
                    testInflatedContainsAllChildren(component2D, cell);
                } catch (Exception e) {
                    String cellDesc = cell + " " + cellLocation(cell) + ": " + e.getMessage();
                    fail("Failed with scale-factor=" + inflation.scaleFactor() + " for cell " + cellDesc);
                }
            }
        }
    }

    private void testInflatedContainsAllChildren(Component2D component2D, String cell) throws Exception {
        if (H3.getResolution(cell) == 5) {
            return;
        }
        String[] children = H3.h3ToChildren(cell);
        for (String child : children) {
            assertThat(
                cell + " " + cellLocation(cell),
                GeoTestUtils.geoShapeValue(getGeoPolygon(child)).relate(component2D),
                not(is(GeoRelation.QUERY_DISJOINT))
            );
            testInflatedContainsAllChildren(component2D, child);
        }
    }

    private Polygon getGeoPolygon(String h3Address) {
        CellBoundary cellBoundary = H3.h3ToGeoBoundary(h3Address);
        double[] x = new double[cellBoundary.numPoints() + 1];
        double[] y = new double[cellBoundary.numPoints() + 1];
        for (int i = 0; i < cellBoundary.numPoints(); i++) {
            LatLng latLng = cellBoundary.getLatLon(i);
            x[i] = latLng.getLonDeg();
            y[i] = latLng.getLatDeg();
        }
        x[x.length - 1] = x[0];
        y[y.length - 1] = y[0];
        LinearRing ring = new LinearRing(x, y);
        return new Polygon(ring);
    }

    public void testChildCoverage() {
        // To use this test to determine the scale factor to use in the H3 grid tiler,
        // increase the cellsPerLevel to 1000 and uncomment the System.out lines at the end of the test.
        int cellPerLevel = 10;
        double totalFactor = 0;
        double maxFactor = 0;
        double minFactor = Float.MAX_VALUE;
        TestExtremeFactorCollector factorCollector = new TestExtremeFactorCollector();
        for (int i = 0; i < cellPerLevel; i++) {
            Point point = randomSafePoint();
            for (int level = 0; level < H3.MAX_H3_RES; level++) {
                double factor = doTestChildCoverage("testChildCoverage" + level, level, point, factorCollector);
                totalFactor += factor;
                if (factor > maxFactor) maxFactor = factor;
                if (factor < minFactor) minFactor = factor;
            }
        }
        double averageFactor = (totalFactor / (cellPerLevel * H3.MAX_H3_RES));
        // Uncomment the following to get the measured scale factor to use in the H3 grid tiler
        // System.out.println("Average factor " + averageFactor);
        // System.out.println("Max factor " + maxFactor);
        // System.out.println("Min factor " + minFactor);
        // factorCollector.dumpStats();
        assertThat("Expected minimum factor", minFactor, greaterThan(1.1));
        assertThat("Expected maximum factor", maxFactor, lessThanOrEqualTo(scaleRecommender().max()));
        assertThat("Expected average factor", averageFactor, lessThan(scaleRecommender().max()));
    }

    private Point randomSafePoint() {
        // We do not use GeometryTestUtils here because we want even distribution over the lat/lon range
        double x = randomDoubleBetween(-180, 180, false);
        double y = randomDoubleBetween(-getLatitudeThreshold(), getLatitudeThreshold(), false);
        return new Point(x, y);
    }

    private int collectOutsidePoints(long[] children, Component2D component, ArrayList<Point> outsideParent) {
        int totalChildVertices = 0;
        for (long child : children) {
            CellBoundary childBoundary = H3.h3ToGeoBoundary(child);
            for (int i = 0; i < childBoundary.numPoints(); i++, totalChildVertices++) {
                LatLng vertex = childBoundary.getLatLon(i);
                Point outside = new Point(vertex.getLonDeg(), vertex.getLatDeg());
                if (outside.getY() < getLatitudeThreshold() && outside.getY() > -getLatitudeThreshold()) {
                    if (component.contains(vertex.getLonDeg(), vertex.getLatDeg()) == false) {
                        outsideParent.add(outside);
                    }
                }
            }
        }
        return totalChildVertices;
    }

    private double doTestChildCoverage(String test, int level, Point point, TestExtremeFactorCollector factorCollector) {
        testGeometryCollector.start(test, 0, 10);
        TestGeometryCollector.Collector collector = testGeometryCollector.normal();
        double maxLat = getLatitudeThreshold();
        double minLat = -maxLat;
        long h3 = H3.geoToH3(point.getLat(), point.getLon(), level);
        String h3Address = H3.h3ToString(h3);
        collector.addH3Cell(h3Address);
        H3LatLonGeometry h3geom = makeGeometry(h3Address);
        Component2D component = h3geom.toComponent2D();
        CellBoundary boundary = H3.h3ToGeoBoundary(h3);
        ArrayList<Point> vertices = new ArrayList<>();
        for (int i = 0; i < boundary.numPoints(); i++) {
            LatLng vertex = boundary.getLatLon(i);
            vertices.add(new Point(vertex.getLonDeg(), vertex.getLatDeg()));
        }
        long[] children = H3.h3ToChildren(h3);
        for (long child : children) {
            collector.addH3Cell(H3.h3ToString(child));
        }
        ArrayList<Point> outsideParent = new ArrayList<>();
        int totalChildVertices = collectOutsidePoints(children, component, outsideParent);
        assertThat(
            "Few child vertices outside the parent for level " + level + " and point " + point,
            1.0 * outsideParent.size() / totalChildVertices,
            both(lessThan(0.35)).and(greaterThan(0.01))
        );
        for (Point childVertex : outsideParent) {
            collector.addPoint(childVertex.getX(), childVertex.getY());
        }
        int countOutside = outsideParent.size();
        double factor = 1.10;
        while (countOutside > 0 && factor < 3.0) {
            factor += 0.001;
            H3LatLonGeometry h3geomScaled = makeGeometry(H3.h3ToString(h3), factor);
            Component2D component2DScaled = h3geomScaled.toComponent2D();
            assertThat("Scaled minX", component2DScaled.getMinX(), either(is(-180d)).or(lessThan(component.getMinX())));
            assertThat("Scaled maxX", component2DScaled.getMaxX(), either(is(180d)).or(greaterThan(component.getMaxX())));
            // assertThat("Scaled minY", component2DScaled.getMinY(), either(is(minLat)).or(lessThan(component.getMinY())));
            // assertThat("Scaled maxY", component2DScaled.getMaxY(), either(is(maxLat)).or(greaterThan(component.getMaxY())));
            ArrayList<Point> outsideParentScaled = new ArrayList<>();
            collectOutsidePoints(children, component2DScaled, outsideParentScaled);
            countOutside = outsideParentScaled.size();
        }
        assertThat("Should have removed all outside children for level " + level + " and point " + point, countOutside, is(0));
        Point centroid = calculateCentroid(boundary);
        centroid = new Point(centroid.getX(), centroid.getY());
        ArrayList<Point> verticesScaled = new ArrayList<>();
        for (Point vertex : vertices) {
            verticesScaled.add(pointInterpolation(centroid, vertex, factor));
            collector.addLine(centroid, verticesScaled.get(verticesScaled.size() - 1));
        }
        // System.out.println(outsideParent.size() + "/" + totalChildVertices + " of child vertices were outside the parent");
        // System.out.println("Got factor " + factor + " for level " + level + " and point " + point);
        factorCollector.addResult(factor, level, h3Address);
        collector.addPolygon(verticesScaled);
        testGeometryCollector.stop((normal, special) -> {
            assertThat(
                String.format(
                    Locale.ROOT,
                    "Two polygons, %d child cells, %d lines and %d child external vertices",
                    children.length,
                    vertices.size(),
                    outsideParent.size()
                ),
                normal.size(),
                is(outsideParent.size() + vertices.size() + children.length + 2)
            );
            assertThat("No special", special.size(), is(0));
        });
        return factor;
    }

    private static class TestExtremeFactorCollector {
        private final HashMap<String, HashSet<String>> factorStats = new HashMap<>();
        private final HashMap<String, HashSet<String>> levelStats = new HashMap<>();
        private final HashMap<String, HashSet<String>> res0Stats = new HashMap<>();

        private void addResult(final double factor, final int level, final String address) {
            String factorKey = String.format(Locale.ROOT, "%.3f", factor);
            factorStats.compute(factorKey, (key, value) -> computeHashSet(value, address));

            String levelKey = String.format(Locale.ROOT, "%.3f-%02d", factor, level);
            levelStats.compute(levelKey, (key, value) -> computeHashSet(value, address));

            String res0Address = address;
            while (H3.getResolution(res0Address) > 0) {
                res0Address = H3.h3ToParent(res0Address);
            }
            String res0Key = String.format(Locale.ROOT, "%.3f-%s", factor, res0Address);
            res0Stats.compute(res0Key, (key, value) -> computeHashSet(value, address));
        }

        private HashSet<String> computeHashSet(HashSet<String> value, String address) {
            if (value == null) {
                value = new HashSet<>();
            }
            value.add(address);
            return value;
        }

        // private void dumpStats() {
        // ArrayList<String> sortedFactors = new ArrayList<>();
        // System.out.println("Stats for factors:");
        // for (String key : factorStats.keySet().stream().sorted(Comparator.reverseOrder()).toList()) {
        // if (sortedFactors.size() < 4 || Double.parseDouble(key) >= 1.3) {
        // sortedFactors.add(key);
        // }
        // System.out.println(key + "\t" + factorStats.get(key).size());
        // }
        // System.out.println("Stats for factors and levels:");
        // for (String key : levelStats.keySet().stream().sorted(Comparator.reverseOrder()).toList()) {
        // String factor = key.substring(0, 5);
        // if (sortedFactors.contains(factor)) {
        // System.out.println(key + "\t" + levelStats.get(key).size());
        // }
        // }
        // System.out.println("Stats for factors and res0 cells:");
        // for (String key : res0Stats.keySet().stream().sorted(Comparator.reverseOrder()).toList()) {
        // String factor = key.substring(0, 5);
        // if (sortedFactors.contains(factor)) {
        // String address = key.substring(6);
        // LatLng latlnt = H3.h3ToLatLng(address);
        // System.out.printf(
        // Locale.ROOT,
        // "%s\t%d\t%s\t%s\n",
        // key,
        // res0Stats.get(key).size(),
        // cellLocation(address),
        // res0Stats.get(key)
        // );
        // }
        // }
        // }
    }

    private static String cellLocation(String address) {
        LatLng point = H3.h3ToLatLng(address);
        return String.format(Locale.ROOT, "POINT( %9.4f %9.4f )", point.getLonDeg(), point.getLatDeg());
    }
}
