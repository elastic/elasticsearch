/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.geo.GeoTestUtil;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.LongConsumer;

import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.LATITUDE_MASK;
import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.NORMALIZED_LATITUDE_MASK;
import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.NORMALIZED_NEGATIVE_LATITUDE_MASK;
import static org.elasticsearch.xpack.spatial.util.GeoTestUtils.encodeDecodeLat;
import static org.elasticsearch.xpack.spatial.util.GeoTestUtils.encodeDecodeLon;
import static org.elasticsearch.xpack.spatial.util.GeoTestUtils.geoShapeValue;
import static org.elasticsearch.xpack.spatial.util.GeoTestUtils.randomBBox;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class GeoGridTilerTests extends ESTestCase {
    private static final LongConsumer NOOP_BREAKER = (l) -> {};

    public void testGeoTile() throws Exception {
        double x = randomDouble();
        double y = randomDouble();
        int precision = randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
        assertThat(new UnBoundedGeoTileGridTiler(precision).encode(x, y), equalTo(GeoTileUtils.longEncode(x, y, precision)));

        // create rectangle within tile and check bound counts
        Rectangle tile = GeoTileUtils.toBoundingBox(1309, 3166, 13);
        Rectangle shapeRectangle = new Rectangle(tile.getMinX() + 0.00001, tile.getMaxX() - 0.00001,
            tile.getMaxY() - 0.00001,  tile.getMinY() + 0.00001);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(shapeRectangle);
        // test shape within tile bounds
        {
            GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), new UnBoundedGeoTileGridTiler(13), NOOP_BREAKER);
            assertTrue(values.advanceExact(0));
            assertThat(values.docValueCount(), equalTo(1));
        }
        {
            GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), new UnBoundedGeoTileGridTiler(14), NOOP_BREAKER);
            assertTrue(values.advanceExact(0));
            assertThat(values.docValueCount(), equalTo(4));
        }
        {
            GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), new UnBoundedGeoTileGridTiler(15), NOOP_BREAKER);
            assertTrue(values.advanceExact(0));
            assertThat(values.docValueCount(), equalTo(16));
        }
    }

    public void testGeoTileSetValuesBruteAndRecursiveMultiline() throws Exception {
        MultiLine geometry = GeometryTestUtils.randomMultiLine(false);
        checkGeoTileSetValuesBruteAndRecursive(geometry);
        checkGeoHashSetValuesBruteAndRecursive(geometry);
    }

    public void testGeoTileSetValuesBruteAndRecursivePolygon() throws Exception {
        Geometry geometry = GeometryTestUtils.randomPolygon(false);
        checkGeoTileSetValuesBruteAndRecursive(geometry);
        checkGeoHashSetValuesBruteAndRecursive(geometry);
    }

    public void testGeoTileSetValuesBruteAndRecursivePoints() throws Exception {
        Geometry geometry = randomBoolean() ? GeometryTestUtils.randomPoint(false) : GeometryTestUtils.randomMultiPoint(false);
        checkGeoTileSetValuesBruteAndRecursive(geometry);
        checkGeoHashSetValuesBruteAndRecursive(geometry);
    }

    // tests that bounding boxes of shapes crossing the dateline are correctly wrapped
    public void testGeoTileSetValuesBoundingBoxes_BoundedGeoShapeCellValues() throws Exception {
        for (int i = 0; i < 1; i++) {
            int precision = randomIntBetween(0, 4);
            GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
            Geometry geometry = indexer.prepareForIndexing(randomValueOtherThanMany(g -> {
                try {
                    indexer.prepareForIndexing(g);
                    return false;
                } catch (Exception e) {
                    return true;
                }
            }, () -> boxToGeo(randomBBox())));

            GeoBoundingBox geoBoundingBox = randomBBox();
            GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);
            GeoShapeCellValues cellValues =
                new GeoShapeCellValues(makeGeoShapeValues(value), new BoundedGeoTileGridTiler(precision, geoBoundingBox), NOOP_BREAKER);

            assertTrue(cellValues.advanceExact(0));
            int numTiles = cellValues.docValueCount();
            int expected = numTiles(value, precision, geoBoundingBox);

            assertThat(numTiles, equalTo(expected));
        }
    }

    // test random rectangles that can cross the date-line and verify that there are an expected
    // number of tiles returned
    public void testGeoTileSetValuesBoundingBoxes_UnboundedGeoShapeCellValues() throws Exception {
        for (int i = 0; i < 1000; i++) {
            int precision = randomIntBetween(0, 4);
            GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
            Geometry geometry = indexer.prepareForIndexing(randomValueOtherThanMany(g -> {
                try {
                    indexer.prepareForIndexing(g);
                    return false;
                } catch (Exception e) {
                    return true;
                }
            }, () -> boxToGeo(randomBBox())));

            GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);
            GeoShapeCellValues unboundedCellValues =
                new GeoShapeCellValues(makeGeoShapeValues(value), new UnBoundedGeoTileGridTiler(precision), NOOP_BREAKER);
            assertTrue(unboundedCellValues.advanceExact(0));
            int numTiles = unboundedCellValues.docValueCount();
            int expected = numTiles(value, precision);
            assertThat(numTiles, equalTo(expected));
            // make sure we are not over-allocating
            assertThat(4 * numTiles + 1, greaterThanOrEqualTo(unboundedCellValues.getValues().length));
        }
    }

    public void testTilerMatchPoint() throws Exception {
        int precision = randomIntBetween(0, 4);
        Point originalPoint = GeometryTestUtils.randomPoint(false);
        int xTile = GeoTileUtils.getXTile(originalPoint.getX(), 1 << precision);
        int yTile = GeoTileUtils.getYTile(originalPoint.getY(), 1 << precision);
        Rectangle bbox = GeoTileUtils.toBoundingBox(xTile, yTile, precision);

        Point[] pointCorners = new Point[] {
            // tile corners
            new Point(bbox.getMinX(), bbox.getMinY()),
            new Point(bbox.getMinX(), bbox.getMaxY()),
            new Point(bbox.getMaxX(), bbox.getMinY()),
            new Point(bbox.getMaxX(), bbox.getMaxY()),
            // tile edge midpoints
            new Point(bbox.getMinX(), (bbox.getMinY() + bbox.getMaxY()) / 2),
            new Point(bbox.getMaxX(), (bbox.getMinY() + bbox.getMaxY()) / 2),
            new Point((bbox.getMinX() + bbox.getMaxX()) / 2, bbox.getMinY()),
            new Point((bbox.getMinX() + bbox.getMaxX()) / 2, bbox.getMaxY()),
        };

        for (Point point : pointCorners) {
            if (point.getX() == GeoUtils.MAX_LON || point.getY() == -LATITUDE_MASK) {
                continue;
            }
            GeoShapeValues.GeoShapeValue value = geoShapeValue(point);
            GeoShapeCellValues unboundedCellValues =
                new GeoShapeCellValues(makeGeoShapeValues(value), new UnBoundedGeoTileGridTiler(precision), NOOP_BREAKER);
            assertTrue(unboundedCellValues.advanceExact(0));
            int numTiles = unboundedCellValues.docValueCount();
            assertThat(numTiles, equalTo(1));
            long tilerHash = unboundedCellValues.getValues()[0];
            long pointHash = GeoTileUtils.longEncode(encodeDecodeLon(point.getX()), encodeDecodeLat(point.getY()), precision);
            assertThat(tilerHash, equalTo(pointHash));
        }
    }

    public void testGeoHash() throws Exception {
        double x = randomDouble();
        double y = randomDouble();
        int precision = randomIntBetween(0, 6);
        assertThat(new UnboundedGeoHashGridTiler(precision).encode(x, y), equalTo(Geohash.longEncode(x, y, precision)));

        Rectangle tile = Geohash.toBoundingBox(Geohash.stringEncode(x, y, 5));

        Rectangle shapeRectangle = new Rectangle(tile.getMinX() + 0.00001, tile.getMaxX() - 0.00001,
            tile.getMaxY() - 0.00001,  tile.getMinY() + 0.00001);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(shapeRectangle);

        // test shape within tile bounds
        {
            GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), new UnboundedGeoHashGridTiler(5), NOOP_BREAKER);
            assertTrue(values.advanceExact(0));
            int count = values.docValueCount();
            assertThat(count, equalTo(1));
        }
        {
            GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), new UnboundedGeoHashGridTiler(6), NOOP_BREAKER);
            assertTrue(values.advanceExact(0));
            int count = values.docValueCount();
            assertThat(count, equalTo(32));
        }
        {
            GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), new UnboundedGeoHashGridTiler(7), NOOP_BREAKER);
            assertTrue(values.advanceExact(0));
            int count = values.docValueCount();
            assertThat(count, equalTo(1024));
        }
    }

    public void testGeoHashBoundsExcludeTouchingTiles() throws Exception {
        final int precision = randomIntBetween(1, 5);
        final String hash =
            Geohash.stringEncode(GeoTestUtil.nextLongitude(), GeoTestUtil.nextLatitude(), precision);

        final Rectangle rectangle = Geohash.toBoundingBox(hash);
        final GeoBoundingBox box = new GeoBoundingBox(
            new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
            new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
        );
        final Rectangle other = new Rectangle(
            Math.max(-180, rectangle.getMinX() - 1),
            Math.min(180, rectangle.getMaxX() + 1),
            Math.min(90, rectangle.getMaxY() + 1),
            Math.max(-90, rectangle.getMinY() - 1));
        final GeoShapeValues.GeoShapeValue value = geoShapeValue(other);
        for (int i = 0;  i < 4; i++) {
            final BoundedGeoHashGridTiler bounded = new BoundedGeoHashGridTiler(precision + i, box);
            final GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), bounded, NOOP_BREAKER);
            assertTrue(values.advanceExact(0));
            final int numTiles = values.docValueCount();
            final int expected = (int) Math.pow(32, i);
            assertThat(numTiles, equalTo(expected));
        }
    }

    public void testGeoTileBoundsExcludeTouchingTiles() throws Exception {
        final int z = randomIntBetween(1, GeoTileUtils.MAX_ZOOM - 10);
        final int x = randomIntBetween(0, (1 << z) - 1);
        final int y = randomIntBetween(0, (1 << z) - 1);
        final Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, z);
        final GeoBoundingBox box = new GeoBoundingBox(
            new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
            new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
        );
        final Rectangle other = new Rectangle(
            Math.max(-180, rectangle.getMinX() - 1),
            Math.min(180, rectangle.getMaxX() + 1),
            Math.min(90, rectangle.getMaxY() + 1),
            Math.max(-90, rectangle.getMinY() - 1));
        final GeoShapeValues.GeoShapeValue value = geoShapeValue(other);
        for (int i = 0;  i < 10; i++) {
            final BoundedGeoTileGridTiler bounded = new BoundedGeoTileGridTiler(z + i, box);
            final GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), bounded, NOOP_BREAKER);
            assertTrue(values.advanceExact(0));
            final int numTiles = values.docValueCount();
            final int expected = 1 << (2 * i);
            assertThat(numTiles, equalTo(expected));
            assertThat((int) bounded.getMaxTiles(), equalTo(expected));
        }
    }

    public void testGeoTileShapeContainsBound() throws Exception {
        Rectangle tile = GeoTileUtils.toBoundingBox(44140, 44140, 16);
        Rectangle shapeRectangle = new Rectangle(tile.getMinX() - 15, tile.getMaxX() + 15,
            tile.getMaxY() + 15, tile.getMinY() - 15);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(shapeRectangle);

        GeoBoundingBox boundingBox = new GeoBoundingBox(
            new GeoPoint(tile.getMaxLat(), tile.getMinLon()),
            new GeoPoint(tile.getMinLat(), tile.getMaxLon())
        );
        BoundedGeoTileGridTiler tiler = new BoundedGeoTileGridTiler(24, boundingBox);
        GeoShapeCellValues values =
            new GeoShapeCellValues(makeGeoShapeValues(value), tiler, NOOP_BREAKER);
        assertTrue(values.advanceExact(0));
        int numTiles = values.docValueCount();
        int expectedTiles = Math.toIntExact(tiler.getMaxTiles());
        assertThat(numTiles, equalTo(expectedTiles));
        assertThat(numTiles, equalTo(256 * 256));
    }

    public void testGeoTileShapeContainsBound3() throws Exception {
        Rectangle tile = GeoTileUtils.toBoundingBox(2, 2, 3);
        Rectangle shapeRectangle = new Rectangle(tile.getMinX() - 1, tile.getMaxX() + 1,
            tile.getMaxY() + 1, tile.getMinY() - 1);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(shapeRectangle);

        GeoBoundingBox boundingBox = new GeoBoundingBox(
            new GeoPoint(tile.getMaxLat(), tile.getMinLon()),
            new GeoPoint(tile.getMinLat(), tile.getMaxLon())
        );
        BoundedGeoTileGridTiler tiler = new BoundedGeoTileGridTiler(4, boundingBox);
        GeoShapeCellValues values =
            new GeoShapeCellValues(makeGeoShapeValues(value), tiler, NOOP_BREAKER);
        assertTrue(values.advanceExact(0));
        int numTiles = values.docValueCount();
        int expectedTiles = Math.toIntExact(tiler.getMaxTiles());
        assertThat(expectedTiles, equalTo(numTiles));
    }

    public void testGeoTileShapeContainsBoundDateLine() throws Exception {
        Rectangle tile = new Rectangle(178, -178, 2, -2);
        Rectangle shapeRectangle = new Rectangle(170, -170, 10, -10);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(shapeRectangle);

        GeoBoundingBox boundingBox = new GeoBoundingBox(
            new GeoPoint(tile.getMaxLat(), tile.getMinLon()),
            new GeoPoint(tile.getMinLat(), tile.getMaxLon())
        );
        BoundedGeoTileGridTiler tiler = new BoundedGeoTileGridTiler(13, boundingBox);
        GeoShapeCellValues values =
            new GeoShapeCellValues(makeGeoShapeValues(value), new BoundedGeoTileGridTiler(13, boundingBox), NOOP_BREAKER);
        assertTrue(values.advanceExact(0));
        int numTiles = values.docValueCount();
        int expectedTiles = Math.toIntExact(tiler.getMaxTiles());
        assertThat(expectedTiles, equalTo(numTiles));
    }

    private boolean tileIntersectsBounds(int x, int y, int precision, GeoBoundingBox bbox) {
        if (bbox == null) {
            return true;
        }
        final int tiles = 1 << precision;
        int minX = GeoTileUtils.getXTile(bbox.left(), tiles);
        int minY = GeoTileUtils.getYTile(bbox.top(), tiles);
        final Rectangle minTile = GeoTileUtils.toBoundingBox(minX, minY, precision);
        if (minTile.getMaxX() == bbox.left()) {
            minX++;
        }
        if (minTile.getMinY() == bbox.top()) {
            minY++;
        }
        // compute maxX, maxY
        int maxX = GeoTileUtils.getXTile(bbox.right(), tiles);
        int maxY = GeoTileUtils.getYTile(bbox.bottom(), tiles);
        final Rectangle maxTile = GeoTileUtils.toBoundingBox(maxX, maxY, precision);
        if (maxTile.getMinX() == bbox.right()) {
            maxX--;
        }
        if (maxTile.getMaxY() == bbox.bottom()) {
            maxY--;
        }
        if (maxY >= y && minY <= y) {
            if (bbox.left() > bbox.right()) {
                return maxX >= x || minX <= x;
            } else {
                return maxX >= x && minX <= x;
            }
        }
        return false;
    }

    private int numTiles(GeoShapeValues.GeoShapeValue geoValue, int precision, GeoBoundingBox geoBox) {
        GeoShapeValues.BoundingBox bounds = geoValue.boundingBox();
        int count = 0;

        if (precision == 0) {
            return 1;
        } else if ((bounds.top > LATITUDE_MASK && bounds.bottom > LATITUDE_MASK)
            || (bounds.top < -LATITUDE_MASK && bounds.bottom < -LATITUDE_MASK)) {
            return 0;
        }
        final double tiles = 1 << precision;
        int minYTile = GeoTileUtils.getYTile(bounds.maxY(), (long) tiles);
        int maxYTile = GeoTileUtils.getYTile(bounds.minY(), (long) tiles);
        if ((bounds.posLeft >= 0 && bounds.posRight >= 0)  && (bounds.negLeft < 0 && bounds.negRight < 0)) {
            // box one
            int minXTileNeg = GeoTileUtils.getXTile(bounds.negLeft, (long) tiles);
            int maxXTileNeg = GeoTileUtils.getXTile(bounds.negRight, (long) tiles);

            for (int x = minXTileNeg; x <= maxXTileNeg; x++) {
                for (int y = minYTile; y <= maxYTile; y++) {
                    Rectangle r = GeoTileUtils.toBoundingBox(x, y, precision);
                    if (tileIntersectsBounds(x, y, precision, geoBox) && geoValue.relate(r) != GeoRelation.QUERY_DISJOINT) {
                        count += 1;
                    }
                }
            }

            // box two
            int minXTilePos = GeoTileUtils.getXTile(bounds.posLeft, (long) tiles);
            if (minXTilePos > maxXTileNeg + 1) {
                minXTilePos -= 1;
            }

            int maxXTilePos = GeoTileUtils.getXTile(bounds.posRight, (long) tiles);

            for (int x = minXTilePos; x <= maxXTilePos; x++) {
                for (int y = minYTile; y <= maxYTile; y++) {
                    Rectangle r = GeoTileUtils.toBoundingBox(x, y, precision);
                    if (tileIntersectsBounds(x, y, precision, geoBox) && geoValue.relate(r) != GeoRelation.QUERY_DISJOINT) {
                        count += 1;
                    }
                }
            }
            return count;
        } else {
            int minXTile = GeoTileUtils.getXTile(bounds.minX(), (long) tiles);
            int maxXTile = GeoTileUtils.getXTile(bounds.maxX(), (long) tiles);

            if (minXTile == maxXTile && minYTile == maxYTile) {
                return tileIntersectsBounds(minXTile, minYTile, precision, geoBox) ? 1 : 0;
            }

            for (int x = minXTile; x <= maxXTile; x++) {
                for (int y = minYTile; y <= maxYTile; y++) {
                    Rectangle r = GeoTileUtils.toBoundingBox(x, y, precision);
                    if (tileIntersectsBounds(x, y, precision, geoBox) && geoValue.relate(r) != GeoRelation.QUERY_DISJOINT) {
                        count += 1;
                    }
                }
            }
            return count;
        }
    }

    private void checkGeoTileSetValuesBruteAndRecursive(Geometry geometry) throws Exception {
        int precision = randomIntBetween(1, 4);
        UnBoundedGeoTileGridTiler tiler = new UnBoundedGeoTileGridTiler(precision);
        GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
        geometry = indexer.prepareForIndexing(geometry);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);
        GeoShapeCellValues recursiveValues = new GeoShapeCellValues(null, tiler, NOOP_BREAKER);
        int recursiveCount;
        {
            recursiveCount = tiler.setValuesByRasterization(0, 0, 0, recursiveValues, 0, value);
        }
        GeoShapeCellValues bruteForceValues = new GeoShapeCellValues(null, tiler, NOOP_BREAKER);
        int bruteForceCount;
        {
            final double tiles = 1 << precision;
            GeoShapeValues.BoundingBox bounds = value.boundingBox();
            int minXTile = GeoTileUtils.getXTile(bounds.minX(), (long) tiles);
            int minYTile = GeoTileUtils.getYTile(bounds.maxY(), (long) tiles);
            int maxXTile = GeoTileUtils.getXTile(bounds.maxX(), (long) tiles);
            int maxYTile = GeoTileUtils.getYTile(bounds.minY(), (long) tiles);
            bruteForceCount = tiler.setValuesByBruteForceScan(bruteForceValues, value, minXTile, minYTile, maxXTile, maxYTile);
        }
        assertThat(geometry.toString(), recursiveCount, equalTo(bruteForceCount));
        long[] recursive = Arrays.copyOf(recursiveValues.getValues(), recursiveCount);
        long[] bruteForce = Arrays.copyOf(bruteForceValues.getValues(), bruteForceCount);
        Arrays.sort(recursive);
        Arrays.sort(bruteForce);
        assertArrayEquals(geometry.toString(), recursive, bruteForce);
    }

    private void checkGeoHashSetValuesBruteAndRecursive(Geometry geometry) throws Exception {
        int precision = randomIntBetween(1, 3);
        UnboundedGeoHashGridTiler tiler = new UnboundedGeoHashGridTiler(precision);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);
        GeoShapeCellValues recursiveValues = new GeoShapeCellValues(null, tiler, NOOP_BREAKER);
        int recursiveCount;
        {
            recursiveCount = tiler.setValuesByRasterization("", recursiveValues, 0, value);
        }
        GeoShapeCellValues bruteForceValues = new GeoShapeCellValues(null, tiler, NOOP_BREAKER);
        int bruteForceCount;
        {
            GeoShapeValues.BoundingBox bounds = value.boundingBox();
            bruteForceCount = tiler.setValuesByBruteForceScan(bruteForceValues, value, bounds);
        }

        assertThat(geometry.toString(), recursiveCount, equalTo(bruteForceCount));

        long[] recursive = Arrays.copyOf(recursiveValues.getValues(), recursiveCount);
        long[] bruteForce = Arrays.copyOf(bruteForceValues.getValues(), bruteForceCount);
        Arrays.sort(recursive);
        Arrays.sort(bruteForce);
        assertArrayEquals(geometry.toString(), recursive, bruteForce);
    }


    static Geometry boxToGeo(GeoBoundingBox geoBox) {
        // turn into polygon
        if (geoBox.right() < geoBox.left() && geoBox.right() != -180) {
            return new MultiPolygon(List.of(
                new Polygon(new LinearRing(
                    new double[] { -180, geoBox.right(), geoBox.right(), -180, -180 },
                    new double[] { geoBox.bottom(), geoBox.bottom(), geoBox.top(), geoBox.top(), geoBox.bottom() })),
                new Polygon(new LinearRing(
                    new double[] { geoBox.left(), 180, 180, geoBox.left(), geoBox.left() },
                    new double[] { geoBox.bottom(), geoBox.bottom(), geoBox.top(), geoBox.top(), geoBox.bottom() }))
            ));
        } else {
            double right = GeoUtils.normalizeLon(geoBox.right());
            return new Polygon(new LinearRing(
                new double[] { geoBox.left(), right, right, geoBox.left(), geoBox.left() },
                new double[] { geoBox.bottom(), geoBox.bottom(), geoBox.top(), geoBox.top(), geoBox.bottom() }));
        }
    }

    private int numTiles(GeoShapeValues.GeoShapeValue geoValue, int precision) {
        GeoShapeValues.BoundingBox bounds = geoValue.boundingBox();
        int count = 0;

        if ((bounds.top > NORMALIZED_LATITUDE_MASK && bounds.bottom > NORMALIZED_LATITUDE_MASK)
            || (bounds.top < NORMALIZED_NEGATIVE_LATITUDE_MASK && bounds.bottom < NORMALIZED_NEGATIVE_LATITUDE_MASK)) {
            return 0;
        }

        if (precision == 0) {
            return 1;
        }

        final double tiles = 1 << precision;
        int minYTile = GeoTileUtils.getYTile(bounds.maxY(), (long) tiles);
        int maxYTile = GeoTileUtils.getYTile(bounds.minY(), (long) tiles);
        if ((bounds.posLeft >= 0 && bounds.posRight >= 0)  && (bounds.negLeft < 0 && bounds.negRight < 0)) {
            // box one
            int minXTileNeg = GeoTileUtils.getXTile(bounds.negLeft, (long) tiles);
            int maxXTileNeg = GeoTileUtils.getXTile(bounds.negRight, (long) tiles);

            for (int x = minXTileNeg; x <= maxXTileNeg; x++) {
                for (int y = minYTile; y <= maxYTile; y++) {
                    Rectangle r = GeoTileUtils.toBoundingBox(x, y, precision);
                    if (geoValue.relate(r) != GeoRelation.QUERY_DISJOINT) {
                        count += 1;
                    }
                }
            }

            // box two
            int minXTilePos = GeoTileUtils.getXTile(bounds.posLeft, (long) tiles);
            if (minXTilePos > maxXTileNeg + 1) {
                minXTilePos -= 1;
            }

            int maxXTilePos = GeoTileUtils.getXTile(bounds.posRight, (long) tiles);

            for (int x = minXTilePos; x <= maxXTilePos; x++) {
                for (int y = minYTile; y <= maxYTile; y++) {
                    Rectangle r = GeoTileUtils.toBoundingBox(x, y, precision);
                    if (geoValue.relate(r) != GeoRelation.QUERY_DISJOINT) {
                        count += 1;
                    }
                }
            }
            return count;
        } else {
            int minXTile = GeoTileUtils.getXTile(bounds.minX(), (long) tiles);
            int maxXTile = GeoTileUtils.getXTile(bounds.maxX(), (long) tiles);

            if (minXTile == maxXTile && minYTile == maxYTile) {
                return 1;
            }

            for (int x = minXTile; x <= maxXTile; x++) {
                for (int y = minYTile; y <= maxYTile; y++) {
                    Rectangle r = GeoTileUtils.toBoundingBox(x, y, precision);
                    if (geoValue.relate(r) != GeoRelation.QUERY_DISJOINT) {
                        count += 1;
                    }
                }
            }
            return count;
        }
    }

    public void testGeoHashGridCircuitBreaker() throws IOException {
        testCircuitBreaker(new UnboundedGeoHashGridTiler(randomIntBetween(0, 3)));
    }

    public void testGeoTileGridCircuitBreaker() throws IOException {
        testCircuitBreaker(new UnBoundedGeoTileGridTiler(randomIntBetween(0, 3)));
    }

    private void testCircuitBreaker(GeoGridTiler tiler) throws IOException {
        Geometry geometry = GeometryTestUtils.randomPolygon(false);

        GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);

        List<Long> byteChangeHistory = new ArrayList<>();
        if (tiler.precision() == 0) {
            new AllCellValues(null, tiler.encode(0, 0), byteChangeHistory::add);
        } else {
            GeoShapeCellValues values = new GeoShapeCellValues(null, tiler, byteChangeHistory::add);
            tiler.setValues(values, value);
        }

        final long maxNumBytes;
        final long curNumBytes;
        if (byteChangeHistory.size() == 1) {
            curNumBytes = maxNumBytes = byteChangeHistory.get(byteChangeHistory.size() - 1);
        } else {
            long oldNumBytes = -byteChangeHistory.get(byteChangeHistory.size() - 1);
            curNumBytes = byteChangeHistory.get(byteChangeHistory.size() - 2);
            maxNumBytes = oldNumBytes + curNumBytes;
        }

        CircuitBreakerService service = new HierarchyCircuitBreakerService(Settings.EMPTY,
            Collections.singletonList(new BreakerSettings("limited", maxNumBytes - 1, 1.0)),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        CircuitBreaker limitedBreaker = service.getBreaker("limited");

        LongConsumer circuitBreakerConsumer = (l) -> limitedBreaker.addEstimateBytesAndMaybeBreak(l, "agg");
        expectThrows(CircuitBreakingException.class, () -> {
            GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), tiler, circuitBreakerConsumer);
            assertTrue(values.advanceExact(0));
            assertThat(values.getValuesBytes(), equalTo(curNumBytes));
            assertThat(limitedBreaker.getUsed(), equalTo(curNumBytes));
        });
    }

    private GeoShapeValues makeGeoShapeValues(GeoShapeValues.GeoShapeValue... values) {
        return new GeoShapeValues() {
            int index = 0;

            @Override
            public boolean advanceExact(int doc) throws IOException {
                assertThat(index, Matchers.greaterThanOrEqualTo(doc));
                if (doc < values.length) {
                    index = doc;
                    return true;
                }
                return false;
            }

            @Override
            public ValuesSourceType valuesSourceType() {
                return GeoShapeValuesSourceType.instance();
            }

            @Override
            public GeoShapeValue value() {
                return values[index];
            }
        };
    }
}
