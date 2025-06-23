/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.h3.H3;
import org.elasticsearch.xpack.spatial.common.H3CartesianUtil;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.xpack.spatial.util.GeoTestUtils.geoShapeValue;
import static org.hamcrest.Matchers.equalTo;

public class GeoHexTilerTests extends GeoGridTilerTestCase<GeoHexGridTiler> {

    @Override
    protected GeoHexGridTiler getGridTiler(GeoBoundingBox bbox, int precision) {
        return GeoHexGridTiler.makeGridTiler(precision, bbox);
    }

    @Override
    protected int maxPrecision() {
        return H3.MAX_H3_RES;
    }

    @Override
    protected Rectangle getCell(double lon, double lat, int precision) {
        return H3CartesianUtil.toBoundingBox(H3.geoToH3(lat, lon, precision));
    }

    /** The H3 tilers does not produce rectangular tiles, and some tests assume this */
    @Override
    protected boolean isRectangularTiler() {
        return false;
    }

    @Override
    protected long getCellsForDiffPrecision(int precisionDiff) {
        return GeoHexGridTiler.calcMaxAddresses(precisionDiff);
    }

    public void testLargeShape() throws Exception {
        // We have a shape and a tile both covering all mercator space, so we expect all level0 H3 cells to match
        Rectangle shapeRectangle = new Rectangle(-180, 180, 90, -90);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(shapeRectangle);

        GeoBoundingBox boundingBox = new GeoBoundingBox(
            new GeoPoint(shapeRectangle.getMaxLat(), shapeRectangle.getMinLon()),
            new GeoPoint(shapeRectangle.getMinLat(), shapeRectangle.getMaxLon())
        );

        for (int precision = 0; precision < 4; precision++) {
            GeoShapeCellValues values = new GeoShapeCellValues(
                makeGeoShapeValues(value),
                getGridTiler(boundingBox, precision),
                NOOP_BREAKER
            );
            assertTrue(values.advanceExact(0));
            int numTiles = values.docValueCount();
            int expectedTiles = expectedBuckets(value, precision, boundingBox);
            assertThat(expectedTiles, equalTo(numTiles));
        }
    }

    public void testLargeShapeWithBounds() throws Exception {
        // We have a shape covering all space
        Rectangle shapeRectangle = new Rectangle(-180, 180, 90, -90);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(shapeRectangle);

        Point point = GeometryTestUtils.randomPoint();
        int res = randomIntBetween(0, H3.MAX_H3_RES - 4);
        long h3 = H3.geoToH3(point.getLat(), point.getLon(), res);
        Rectangle tile = H3CartesianUtil.toBoundingBox(h3);
        GeoBoundingBox boundingBox = new GeoBoundingBox(
            new GeoPoint(
                GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(tile.getMaxLat())),
                GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(tile.getMinLon()))
            ),
            new GeoPoint(
                GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(tile.getMinLat())),
                GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(tile.getMaxLon()))
            )
        );

        for (int precision = res; precision < res + 4; precision++) {
            String msg = "Failed " + WellKnownText.toWKT(point) + " at resolution " + res + " with precision " + precision;
            GeoShapeCellValues values = new GeoShapeCellValues(
                makeGeoShapeValues(value),
                getGridTiler(boundingBox, precision),
                NOOP_BREAKER
            );
            assertTrue(values.advanceExact(0));
            long[] h3bins = ArrayUtil.copyOfSubArray(values.getValues(), 0, values.docValueCount());
            assertCorner(h3bins, new Point(tile.getMinLon(), tile.getMinLat()), precision, msg);
            assertCorner(h3bins, new Point(tile.getMaxLon(), tile.getMinLat()), precision, msg);
            assertCorner(h3bins, new Point(tile.getMinLon(), tile.getMaxLat()), precision, msg);
            assertCorner(h3bins, new Point(tile.getMaxLon(), tile.getMaxLat()), precision, msg);
        }
    }

    // Polygons with bounds inside the South Pole cell break a tiler optimization
    public void testTroublesomeShapeAlmostWithinSouthPole_BoundedGeoShapeCellValues() throws Exception {
        int precision = 1;
        String polygon = """
            POLYGON((180.0 -90.0, 180.0 -73.80002960532788, 1.401298464324817E-45 -73.80002960532788,
            1.401298464324817E-45 -90.0, 180.0 -90.0))""";
        GeoBoundingBox geoBoundingBox = new GeoBoundingBox(
            new GeoPoint(19.585157879020088, 0.9999999403953552),
            new GeoPoint(-90.0, -26.405694642531472)
        );
        Geometry geometry = WellKnownText.fromWKT(StandardValidator.instance(true), true, polygon);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);
        GeoShapeCellValues cellValues = new GeoShapeCellValues(
            makeGeoShapeValues(value),
            getGridTiler(geoBoundingBox, precision),
            NOOP_BREAKER
        );

        assertTrue(cellValues.advanceExact(0));
        int numBuckets = cellValues.docValueCount();
        int expected = expectedBuckets(value, precision, geoBoundingBox);
        assertThat("[" + precision + "] bucket count", numBuckets, equalTo(expected));
    }

    // Polygons with bounds inside the South Pole cell break a tiler optimization
    public void testTroublesomeShapeAlmostWithinSouthPoleCell_UnboundedGeoShapeCellValues() throws Exception {
        int precision = 0;
        String polygon = """
            POLYGON((1.7481549674935762E-110 -90.0, 180.0 -90.0, 180.0 -75.113250736563,
            1.7481549674935762E-110 -75.113250736563, 1.7481549674935762E-110 -90.0))""";
        Geometry geometry = WellKnownText.fromWKT(StandardValidator.instance(true), true, polygon);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);
        GeoShapeCellValues unboundedCellValues = new GeoShapeCellValues(makeGeoShapeValues(value), getGridTiler(precision), NOOP_BREAKER);

        assertTrue(unboundedCellValues.advanceExact(0));
        int numBuckets = unboundedCellValues.docValueCount();
        int expected = expectedBuckets(value, precision, null);
        assertThat("[" + precision + "] bucket count", numBuckets, equalTo(expected));
    }

    // Polygons with bounds inside the North Pole cell break a tiler optimization
    public void testTroublesomeShapeAlmostWithinNorthPoleCell_UnboundedGeoShapeCellValues() throws Exception {
        int precision = 1;
        String polygon = """
            POLYGON((36.98661841690625 69.44049730644747, 180.0 69.44049730644747,
            180.0 90.0, 36.98661841690625 90.0, 36.98661841690625 69.44049730644747))""";
        Geometry geometry = WellKnownText.fromWKT(StandardValidator.instance(true), true, polygon);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);
        GeoShapeCellValues unboundedCellValues = new GeoShapeCellValues(makeGeoShapeValues(value), getGridTiler(precision), NOOP_BREAKER);

        assertTrue(unboundedCellValues.advanceExact(0));
        int numBuckets = unboundedCellValues.docValueCount();
        int expected = expectedBuckets(value, precision, null);
        assertThat("[" + precision + "] bucket count", numBuckets, equalTo(expected));
    }

    public void testTroublesomePolarCellLevel1_UnboundedGeoShapeCellValues() throws Exception {
        int precision = 1;
        String polygon = "BBOX (-84.24596376729815, 43.36113427778119, 90.0, 83.51476833522361)";
        Geometry geometry = WellKnownText.fromWKT(StandardValidator.instance(true), true, polygon);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);
        GeoShapeCellValues unboundedCellValues = new GeoShapeCellValues(makeGeoShapeValues(value), getGridTiler(precision), NOOP_BREAKER);

        assertTrue(unboundedCellValues.advanceExact(0));
        int numBuckets = unboundedCellValues.docValueCount();
        int expected = expectedBuckets(value, precision, null);
        assertThat("[" + precision + "] bucket count", numBuckets, equalTo(expected));
    }

    public void testTroublesomeCellLevel2_BoundedGeoShapeCellValues() throws Exception {
        int precision = 2;
        String wkt = """
            GEOMETRYCOLLECTION (
              GEOMETRYCOLLECTION (
                POINT(-170 0),
                POINT (-178.5 0)
              )
            )
            """;
        GeoBoundingBox boundingBox = new GeoBoundingBox(new GeoPoint(4E-4, 179.999), new GeoPoint(-4E-4, -179.999));
        Geometry geometry = WellKnownText.fromWKT(StandardValidator.instance(true), true, wkt);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);
        GeoShapeCellValues cellValues = new GeoShapeCellValues(
            makeGeoShapeValues(value),
            getGridTiler(boundingBox, precision),
            NOOP_BREAKER
        );

        assertTrue(cellValues.advanceExact(0));
        int numBuckets = cellValues.docValueCount();
        int expected = expectedBuckets(value, precision, boundingBox);
        assertThat("[" + precision + "] bucket count", numBuckets, equalTo(expected));
    }

    public void testTroublesomeCellLevel4_BoundedGeoShapeCellValues() throws Exception {
        int precision = 4;
        String polygon = "POLYGON ((150.0 70.0, 150.0 85.91811374669217, 168.77544806565834 85.91811374669217, 150.0 70.0))";
        Geometry geometry = WellKnownText.fromWKT(StandardValidator.instance(true), true, polygon);
        GeoBoundingBox boundingBox = new GeoBoundingBox(
            new GeoPoint(86.17678739494652, 172.21916569181505),
            new GeoPoint(83.01600086049713, 179)
        );
        GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);
        GeoShapeCellValues cellValues = new GeoShapeCellValues(
            makeGeoShapeValues(value),
            getGridTiler(boundingBox, precision),
            NOOP_BREAKER
        );

        assertTrue(cellValues.advanceExact(0));
        int numBuckets = cellValues.docValueCount();
        int expected = expectedBuckets(value, precision, boundingBox);
        assertThat("[" + precision + "] bucket count", numBuckets, equalTo(expected));
    }

    public void testIssue96057() throws Exception {
        int precision = 3;
        Geometry geometry = new Polygon(
            new LinearRing(
                new double[] { 47.0, 47.0, -98.41711495022405, -98.41711495022405, 47.0 },
                new double[] { -43.27504297314639, 23.280704041384652, 23.280704041384652, -43.27504297314639, -43.27504297314639 }
            )
        );
        GeoBoundingBox geoBoundingBox = new GeoBoundingBox(
            new GeoPoint(-44.363846082646845, 55.61563600452277),
            new GeoPoint(-75.8747796394427, 42.12290817616412)
        );
        GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);
        GeoShapeCellValues cellValues = new GeoShapeCellValues(
            makeGeoShapeValues(value),
            getGridTiler(geoBoundingBox, precision),
            NOOP_BREAKER
        );

        assertTrue(cellValues.advanceExact(0));
        int numBuckets = cellValues.docValueCount();
        int expected = expectedBuckets(value, precision, geoBoundingBox);
        assertThat(numBuckets, equalTo(expected));
    }

    private void assertCorner(long[] h3bins, Point point, int precision, String msg) throws IOException {
        GeoShapeValues.GeoShapeValue cornerValue = geoShapeValue(point);
        GeoShapeCellValues cornerValues = new GeoShapeCellValues(makeGeoShapeValues(cornerValue), getGridTiler(precision), NOOP_BREAKER);
        assertTrue(cornerValues.advanceExact(0));
        long[] h3binsCorner = ArrayUtil.copyOfSubArray(cornerValues.getValues(), 0, cornerValues.docValueCount());
        for (long corner : h3binsCorner) {
            assertTrue(msg, Arrays.binarySearch(h3bins, corner) != -1);
        }
    }

    @Override
    protected void assertSetValuesBruteAndRecursive(Geometry geometry) throws Exception {
        int precision = randomIntBetween(1, 4);
        GeoHexGridTiler tiler = getGridTiler(precision);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);

        GeoShapeCellValues recursiveValues = new GeoShapeCellValues(null, tiler, NOOP_BREAKER);
        int recursiveCount = tiler.setValuesByRecursion(recursiveValues, value, value.boundingBox());

        GeoShapeCellValues bruteForceValues = new GeoShapeCellValues(null, tiler, NOOP_BREAKER);
        int bruteForceCount = 0;
        for (long h3 : H3.getLongRes0Cells()) {
            bruteForceCount = addBruteForce(tiler, bruteForceValues, value, h3, precision, bruteForceCount);
        }

        long[] recursive = Arrays.copyOf(recursiveValues.getValues(), recursiveCount);
        long[] bruteForce = Arrays.copyOf(bruteForceValues.getValues(), bruteForceCount);

        Arrays.sort(recursive);
        Arrays.sort(bruteForce);
        assertArrayEquals(geometry.toString(), recursive, bruteForce);
    }

    private int addBruteForce(
        GeoHexGridTiler tiler,
        GeoShapeCellValues values,
        GeoShapeValues.GeoShapeValue geoValue,
        long h3,
        int precision,
        int valueIndex
    ) throws IOException {
        if (H3.getResolution(h3) == precision) {
            if (tiler.relateTile(geoValue, h3) != GeoRelation.QUERY_DISJOINT) {
                values.resizeCell(valueIndex + 1);
                values.add(valueIndex++, h3);
            }
        } else {
            for (long child : H3.h3ToChildren(h3)) {
                valueIndex = addBruteForce(tiler, values, geoValue, child, precision, valueIndex);
            }
        }
        return valueIndex;
    }

    @Override
    protected int expectedBuckets(GeoShapeValues.GeoShapeValue geoValue, int precision, GeoBoundingBox bbox) throws Exception {
        GeoHexGridTiler bounded = bbox == null ? null : getGridTiler(bbox, precision);
        GeoHexGridTiler predicate = getGridTiler(precision);
        return computeBuckets(H3.getLongRes0Cells(), bounded, predicate, geoValue, precision);
    }

    private int computeBuckets(
        long[] children,
        GeoHexGridTiler bounded,
        GeoHexGridTiler predicate,
        GeoShapeValues.GeoShapeValue geoValue,
        int finalPrecision
    ) throws IOException {
        int count = 0;
        for (long child : children) {
            if (H3.getResolution(child) == finalPrecision) {
                if (intersects(child, geoValue, bounded, predicate)) {
                    count++;
                }
            } else {
                count += computeBuckets(H3.h3ToChildren(child), bounded, predicate, geoValue, finalPrecision);
            }
        }
        return count;
    }

    private boolean intersects(long h3, GeoShapeValues.GeoShapeValue geoValue, GeoHexGridTiler bounded, GeoHexGridTiler predicate)
        throws IOException {
        if (bounded != null && bounded.h3IntersectsBounds(h3) == false) {
            return false;
        }
        return predicate.relateTile(geoValue, h3) != GeoRelation.QUERY_DISJOINT;
    }
}
