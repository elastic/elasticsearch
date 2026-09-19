/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.common.spatial;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.h3.H3;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Ported from {@code GeoHexTilerTests} in the spatial module: every case checks the recursive search of
 * {@link GeoHexGridTiler} against a brute force enumeration of all cells at the target resolution using the same
 * relation test, so the shortcuts (single cell plus ring, resolution 0 cell plus ring, descend without tests when
 * inside) never lose or invent a cell.
 */
public class GeoHexGridTilerTests extends ESTestCase {
    private static final GeoShapeIndexer INDEXER = new GeoShapeIndexer(Orientation.CCW, "test");
    private static final int NO_LIMIT = Integer.MAX_VALUE;

    static GeoShapeDocValues shape(Geometry geometry) throws IOException {
        return GeoShapeDocValues.from(new BytesRef(WellKnownBinary.toWKB(geometry, ByteOrder.LITTLE_ENDIAN)), INDEXER);
    }

    static Geometry wkt(String wkt) throws Exception {
        return WellKnownText.fromWKT(StandardValidator.instance(true), true, wkt);
    }

    public void testBruteAndRecursivePoint() throws Exception {
        assertBruteAndRecursive(GeometryTestUtils.randomPoint(false));
    }

    public void testBruteAndRecursiveMultiPoint() throws Exception {
        assertBruteAndRecursive(GeometryTestUtils.randomMultiPoint(false));
    }

    public void testBruteAndRecursiveLine() throws Exception {
        assertBruteAndRecursive(GeometryTestUtils.randomLine(false));
    }

    public void testBruteAndRecursiveMultiLine() throws Exception {
        assertBruteAndRecursive(GeometryTestUtils.randomMultiLine(false));
    }

    public void testBruteAndRecursivePolygon() throws Exception {
        assertBruteAndRecursive(GeometryTestUtils.randomPolygon(false));
    }

    public void testBruteAndRecursiveRectangle() throws Exception {
        assertBruteAndRecursive(GeometryTestUtils.randomRectangle());
    }

    public void testLargeShape() throws Exception {
        // A shape and bounds both covering the whole world, so every cell matches
        Rectangle world = new Rectangle(-180, 180, 90, -90);
        GeoBoundingBox bbox = new GeoBoundingBox(new GeoPoint(90, -180), new GeoPoint(-90, 180));
        for (int precision = 0; precision < 4; precision++) {
            assertBucketCount(world, precision, bbox);
            assertBucketCount(world, precision, null);
        }
    }

    public void testLargeShapeWithBounds() throws Exception {
        // A shape covering all space with bounds the size of a random cell: the cell's corners must all be covered
        Rectangle world = new Rectangle(-180, 180, 90, -90);
        Point point = GeometryTestUtils.randomPoint();
        int res = randomIntBetween(0, H3.MAX_H3_RES - 4);
        Rectangle tile = H3CartesianUtil.toBoundingBox(H3.geoToH3(point.getLat(), point.getLon(), res));
        GeoBoundingBox bbox = new GeoBoundingBox(
            new GeoPoint(quantizeLat(tile.getMaxLat()), quantizeLon(tile.getMinLon())),
            new GeoPoint(quantizeLat(tile.getMinLat()), quantizeLon(tile.getMaxLon()))
        );
        for (int precision = res; precision < res + 4; precision++) {
            String msg = "Failed " + WellKnownText.toWKT(point) + " at resolution " + res + " with precision " + precision;
            List<Long> cells = GeoHexGridTiler.makeGridTiler(precision, bbox).cells(shape(world), NO_LIMIT, m -> {});
            assertCorner(cells, new Point(tile.getMinLon(), tile.getMinLat()), precision, msg);
            assertCorner(cells, new Point(tile.getMaxLon(), tile.getMinLat()), precision, msg);
            assertCorner(cells, new Point(tile.getMinLon(), tile.getMaxLat()), precision, msg);
            assertCorner(cells, new Point(tile.getMaxLon(), tile.getMaxLat()), precision, msg);
        }
    }

    // Polygons with bounds inside the South Pole cell break a tiler optimization
    public void testTroublesomeShapeAlmostWithinSouthPoleBounded() throws Exception {
        Geometry geometry = wkt("""
            POLYGON((180.0 -90.0, 180.0 -73.80002960532788, 1.401298464324817E-45 -73.80002960532788,
            1.401298464324817E-45 -90.0, 180.0 -90.0))""");
        GeoBoundingBox bbox = new GeoBoundingBox(
            new GeoPoint(19.585157879020088, 0.9999999403953552),
            new GeoPoint(-90.0, -26.405694642531472)
        );
        assertBucketCount(geometry, 1, bbox);
    }

    public void testTroublesomeShapeAlmostWithinSouthPoleCellUnbounded() throws Exception {
        Geometry geometry = wkt("""
            POLYGON((1.7481549674935762E-110 -90.0, 180.0 -90.0, 180.0 -75.113250736563,
            1.7481549674935762E-110 -75.113250736563, 1.7481549674935762E-110 -90.0))""");
        assertBucketCount(geometry, 0, null);
    }

    public void testTroublesomeShapeAlmostWithinNorthPoleCellUnbounded() throws Exception {
        Geometry geometry = wkt("""
            POLYGON((36.98661841690625 69.44049730644747, 180.0 69.44049730644747,
            180.0 90.0, 36.98661841690625 90.0, 36.98661841690625 69.44049730644747))""");
        assertBucketCount(geometry, 1, null);
    }

    public void testTroublesomePolarCellLevel1Unbounded() throws Exception {
        assertBucketCount(wkt("BBOX (-84.24596376729815, 43.36113427778119, 90.0, 83.51476833522361)"), 1, null);
    }

    public void testTroublesomeCellLevel2Bounded() throws Exception {
        Geometry geometry = wkt("GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT(-170 0), POINT (-178.5 0)))");
        GeoBoundingBox bbox = new GeoBoundingBox(new GeoPoint(4E-4, 179.999), new GeoPoint(-4E-4, -179.999));
        assertBucketCount(geometry, 2, bbox);
    }

    public void testTroublesomeCellLevel4Bounded() throws Exception {
        Geometry geometry = wkt("POLYGON ((150.0 70.0, 150.0 85.91811374669217, 168.77544806565834 85.91811374669217, 150.0 70.0))");
        GeoBoundingBox bbox = new GeoBoundingBox(new GeoPoint(86.17678739494652, 172.21916569181505), new GeoPoint(83.01600086049713, 179));
        assertBucketCount(geometry, 4, bbox);
    }

    public void testIssue96057() throws Exception {
        Geometry geometry = new Polygon(
            new LinearRing(
                new double[] { 47.0, 47.0, -98.41711495022405, -98.41711495022405, 47.0 },
                new double[] { -43.27504297314639, 23.280704041384652, 23.280704041384652, -43.27504297314639, -43.27504297314639 }
            )
        );
        GeoBoundingBox bbox = new GeoBoundingBox(
            new GeoPoint(-44.363846082646845, 55.61563600452277),
            new GeoPoint(-75.8747796394427, 42.12290817616412)
        );
        assertBucketCount(geometry, 3, bbox);
    }

    /** Truncation keeps the first cells found and reports once. */
    public void testTruncation() throws Exception {
        Rectangle world = new Rectangle(-180, 180, 90, -90);
        List<String> warnings = new ArrayList<>();
        List<Long> cells = GeoHexGridTiler.makeGridTiler(2, null).cells(shape(world), 100, warnings::add);
        assertThat(cells, hasSize(100));
        assertThat(warnings, equalTo(List.of("ST_GEOHEX generated more than 100 grid cells")));
    }

    private void assertBruteAndRecursive(Geometry geometry) throws Exception {
        int precision = randomIntBetween(1, 4);
        GeoHexGridTiler tiler = GeoHexGridTiler.makeGridTiler(precision, null);
        GeoShapeDocValues shape = shape(geometry);
        GeoHexGridTiler.Cells recursive = new GeoHexGridTiler.Cells(NO_LIMIT, m -> {});
        tiler.setValuesByRecursion(recursive, shape);
        List<Long> bruteForce = new ArrayList<>();
        for (long h3 : H3.getLongRes0Cells()) {
            addBruteForce(tiler, bruteForce, shape, h3, precision);
        }
        assertThat(geometry.toString(), sorted(recursive.list()), equalTo(sorted(bruteForce)));
    }

    private static void addBruteForce(GeoHexGridTiler tiler, List<Long> cells, GeoShapeDocValues shape, long h3, int precision)
        throws IOException {
        if (H3.getResolution(h3) == precision) {
            if (tiler.relateTile(shape, h3) != GeoRelation.QUERY_DISJOINT) {
                cells.add(h3);
            }
        } else {
            for (long child : H3.h3ToChildren(h3)) {
                addBruteForce(tiler, cells, shape, child, precision);
            }
        }
    }

    private void assertBucketCount(Geometry geometry, int precision, GeoBoundingBox bbox) throws Exception {
        GeoShapeDocValues shape = shape(geometry);
        List<Long> cells = GeoHexGridTiler.makeGridTiler(precision, bbox).cells(shape, NO_LIMIT, m -> {});
        GeoHexGridTiler bounded = bbox == null ? null : GeoHexGridTiler.makeGridTiler(precision, bbox);
        GeoHexGridTiler predicate = GeoHexGridTiler.makeGridTiler(precision, null);
        int expected = computeBuckets(H3.getLongRes0Cells(), bounded, predicate, shape, precision);
        assertThat("[" + precision + "] bucket count", cells.size(), equalTo(expected));
    }

    private static int computeBuckets(
        long[] children,
        GeoHexGridTiler bounded,
        GeoHexGridTiler predicate,
        GeoShapeDocValues shape,
        int precision
    ) throws IOException {
        int count = 0;
        for (long child : children) {
            if (H3.getResolution(child) == precision) {
                if ((bounded == null || bounded.h3IntersectsBounds(child))
                    && predicate.relateTile(shape, child) != GeoRelation.QUERY_DISJOINT) {
                    count++;
                }
            } else {
                count += computeBuckets(H3.h3ToChildren(child), bounded, predicate, shape, precision);
            }
        }
        return count;
    }

    private void assertCorner(List<Long> cells, Point point, int precision, String msg) throws IOException {
        List<Long> cornerCells = GeoHexGridTiler.makeGridTiler(precision, null).cells(shape(point), NO_LIMIT, m -> {});
        for (long corner : cornerCells) {
            assertTrue(msg, cells.contains(corner));
        }
    }

    private static List<Long> sorted(List<Long> cells) {
        List<Long> copy = new ArrayList<>(cells);
        copy.sort(null);
        return copy;
    }

    private static double quantizeLat(double lat) {
        return GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));
    }

    private static double quantizeLon(double lon) {
        return GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lon));
    }
}
