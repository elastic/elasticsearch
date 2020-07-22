/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.geo.GeoBoundingBox;
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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeCoordinateEncoder;
import org.elasticsearch.xpack.spatial.index.fielddata.MultiGeoShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.TriangleTreeReader;

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
import static org.elasticsearch.xpack.spatial.util.GeoTestUtils.randomBBox;
import static org.elasticsearch.xpack.spatial.util.GeoTestUtils.triangleTreeReader;
import static org.hamcrest.Matchers.equalTo;

public class GeoGridTilerTests extends ESTestCase {
    private static final GeoTileGridTiler GEOTILE = new GeoTileGridTiler();
    private static final GeoHashGridTiler GEOHASH = new GeoHashGridTiler();
    private static final LongConsumer NOOP_BREAKER = (l) -> {};

    public void testGeoTile() throws Exception {
        double x = randomDouble();
        double y = randomDouble();
        int precision = randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
        assertThat(GEOTILE.encode(x, y, precision), equalTo(GeoTileUtils.longEncode(x, y, precision)));

        // create rectangle within tile and check bound counts
        Rectangle tile = GeoTileUtils.toBoundingBox(1309, 3166, 13);
        Rectangle shapeRectangle = new Rectangle(tile.getMinX() + 0.00001, tile.getMaxX() - 0.00001,
            tile.getMaxY() - 0.00001,  tile.getMinY() + 0.00001);
        TriangleTreeReader reader = triangleTreeReader(shapeRectangle, GeoShapeCoordinateEncoder.INSTANCE);
        MultiGeoShapeValues.GeoShapeValue value =  new MultiGeoShapeValues.GeoShapeValue(reader);

        // test shape within tile bounds
        {
            GeoShapeCellValues values = new GeoShapeCellValues(null, precision, GEOTILE, NOOP_BREAKER);
            int count = GEOTILE.setValues(values, value, 13);
            assertThat(count, equalTo(1));
        }
        {
            GeoShapeCellValues values = new GeoShapeCellValues(null, precision, GEOTILE, NOOP_BREAKER);
            int count = GEOTILE.setValues(values, value, 14);
            assertThat(count, equalTo(4));
        }
        {
            GeoShapeCellValues values = new GeoShapeCellValues(null, precision, GEOTILE, NOOP_BREAKER);
            int count = GEOTILE.setValues(values, value, 15);
            assertThat(count, equalTo(16));
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

            TriangleTreeReader reader = triangleTreeReader(geometry, GeoShapeCoordinateEncoder.INSTANCE);
            GeoBoundingBox geoBoundingBox = randomBBox();
            MultiGeoShapeValues.GeoShapeValue value = new MultiGeoShapeValues.GeoShapeValue(reader);
            GeoShapeCellValues cellValues = new GeoShapeCellValues(null, precision, GEOTILE, NOOP_BREAKER);

            int numTiles = new BoundedGeoTileGridTiler(geoBoundingBox).setValues(cellValues, value, precision);
            int expected = numTiles(value, precision, geoBoundingBox);

            assertThat(numTiles, equalTo(expected));
        }
    }

    // test random rectangles that can cross the date-line and verify that there are an expected
    // number of tiles returned
    public void testGeoTileSetValuesBoundingBoxes_UnboundedGeoShapeCellValues() throws Exception {
        for (int i = 0; i < 100; i++) {
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

            TriangleTreeReader reader = triangleTreeReader(geometry, GeoShapeCoordinateEncoder.INSTANCE);
            MultiGeoShapeValues.GeoShapeValue value = new MultiGeoShapeValues.GeoShapeValue(reader);
            GeoShapeCellValues unboundedCellValues = new GeoShapeCellValues(null, precision, GEOTILE, NOOP_BREAKER);
            int numTiles = GEOTILE.setValues(unboundedCellValues, value, precision);
            int expected = numTiles(value, precision);
            assertThat(numTiles, equalTo(expected));
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
            TriangleTreeReader reader = triangleTreeReader(point, GeoShapeCoordinateEncoder.INSTANCE);
            MultiGeoShapeValues.GeoShapeValue value = new MultiGeoShapeValues.GeoShapeValue(reader);
            GeoShapeCellValues unboundedCellValues = new GeoShapeCellValues(null, precision, GEOTILE, NOOP_BREAKER);
            int numTiles = GEOTILE.setValues(unboundedCellValues, value, precision);
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
        assertThat(GEOHASH.encode(x, y, precision), equalTo(Geohash.longEncode(x, y, precision)));

        Rectangle tile = Geohash.toBoundingBox(Geohash.stringEncode(x, y, 5));

        Rectangle shapeRectangle = new Rectangle(tile.getMinX() + 0.00001, tile.getMaxX() - 0.00001,
            tile.getMaxY() - 0.00001,  tile.getMinY() + 0.00001);
        TriangleTreeReader reader = triangleTreeReader(shapeRectangle, GeoShapeCoordinateEncoder.INSTANCE);
        MultiGeoShapeValues.GeoShapeValue value =  new MultiGeoShapeValues.GeoShapeValue(reader);

        // test shape within tile bounds
        {
            GeoShapeCellValues values = new GeoShapeCellValues(null, precision, GEOHASH, NOOP_BREAKER);
            int count = GEOHASH.setValues(values, value, 5);
            assertThat(count, equalTo(1));
        }
        {
            GeoShapeCellValues values = new GeoShapeCellValues(null, precision, GEOHASH, NOOP_BREAKER);
            int count = GEOHASH.setValues(values, value, 6);
            assertThat(count, equalTo(32));
        }
        {
            GeoShapeCellValues values = new GeoShapeCellValues(null, precision, GEOHASH, NOOP_BREAKER);
            int count = GEOHASH.setValues(values, value, 7);
            assertThat(count, equalTo(1024));
        }
    }

    private boolean tileIntersectsBounds(int x, int y, int precision, GeoBoundingBox bounds) {
        if (bounds == null) {
            return true;
        }
        final double boundsWestLeft;
        final double boundsWestRight;
        final double boundsEastLeft;
        final double boundsEastRight;
        final boolean crossesDateline;
        if (bounds.right() < bounds.left()) {
            boundsWestLeft = -180;
            boundsWestRight = bounds.right();
            boundsEastLeft = bounds.left();
            boundsEastRight = 180;
            crossesDateline = true;
        } else {
            boundsEastLeft = bounds.left();
            boundsEastRight = bounds.right();
            boundsWestLeft = 0;
            boundsWestRight = 0;
            crossesDateline = false;
        }

        Rectangle tile = GeoTileUtils.toBoundingBox(x, y, precision);

        return (bounds.top() >= tile.getMinY() && bounds.bottom() <= tile.getMaxY()
            && (boundsEastLeft <= tile.getMaxX() && boundsEastRight >= tile.getMinX()
            || (crossesDateline && boundsWestLeft <= tile.getMaxX() && boundsWestRight >= tile.getMinX())));
    }

    private int numTiles(MultiGeoShapeValues.GeoShapeValue geoValue, int precision, GeoBoundingBox geoBox) throws Exception {
        MultiGeoShapeValues.BoundingBox bounds = geoValue.boundingBox();
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
        GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
        geometry = indexer.prepareForIndexing(geometry);
        TriangleTreeReader reader = triangleTreeReader(geometry, GeoShapeCoordinateEncoder.INSTANCE);
        MultiGeoShapeValues.GeoShapeValue value = new MultiGeoShapeValues.GeoShapeValue(reader);
        GeoShapeCellValues recursiveValues = new GeoShapeCellValues(null, precision, GEOTILE, NOOP_BREAKER);
        int recursiveCount;
        {
            recursiveCount = GEOTILE.setValuesByRasterization(0, 0, 0, recursiveValues, 0, precision, value);
        }
        GeoShapeCellValues bruteForceValues = new GeoShapeCellValues(null, precision, GEOTILE, NOOP_BREAKER);
        int bruteForceCount;
        {
            final double tiles = 1 << precision;
            MultiGeoShapeValues.BoundingBox bounds = value.boundingBox();
            int minXTile = GeoTileUtils.getXTile(bounds.minX(), (long) tiles);
            int minYTile = GeoTileUtils.getYTile(bounds.maxY(), (long) tiles);
            int maxXTile = GeoTileUtils.getXTile(bounds.maxX(), (long) tiles);
            int maxYTile = GeoTileUtils.getYTile(bounds.minY(), (long) tiles);
            bruteForceCount = GEOTILE.setValuesByBruteForceScan(bruteForceValues, value, precision, minXTile, minYTile, maxXTile, maxYTile);
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
        GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
        geometry = indexer.prepareForIndexing(geometry);
        TriangleTreeReader reader = triangleTreeReader(geometry, GeoShapeCoordinateEncoder.INSTANCE);
        MultiGeoShapeValues.GeoShapeValue value = new MultiGeoShapeValues.GeoShapeValue(reader);
        GeoShapeCellValues recursiveValues = new GeoShapeCellValues(null, precision, GEOHASH, NOOP_BREAKER);
        int recursiveCount;
        {
            recursiveCount = GEOHASH.setValuesByRasterization("", recursiveValues, 0, precision, value);
        }
        GeoShapeCellValues bruteForceValues = new GeoShapeCellValues(null, precision, GEOHASH, NOOP_BREAKER);
        int bruteForceCount;
        {
            MultiGeoShapeValues.BoundingBox bounds = value.boundingBox();
            bruteForceCount = GEOHASH.setValuesByBruteForceScan(bruteForceValues, value, precision, bounds);
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

    private int numTiles(MultiGeoShapeValues.GeoShapeValue geoValue, int precision) {
        MultiGeoShapeValues.BoundingBox bounds = geoValue.boundingBox();
        int count = 0;

        if (precision == 0) {
            return 1;
        }

        if ((bounds.top > NORMALIZED_LATITUDE_MASK && bounds.bottom > NORMALIZED_LATITUDE_MASK)
            || (bounds.top < NORMALIZED_NEGATIVE_LATITUDE_MASK && bounds.bottom < NORMALIZED_NEGATIVE_LATITUDE_MASK)) {
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
        testCircuitBreaker(GEOHASH);
    }

    public void testGeoTileGridCircuitBreaker() throws IOException {
        testCircuitBreaker(GEOTILE);
    }

    private void testCircuitBreaker(GeoGridTiler tiler) throws IOException {
        Geometry geometry = GeometryTestUtils.randomPolygon(false);
        int precision = randomIntBetween(0, 3);
        TriangleTreeReader reader = triangleTreeReader(geometry, GeoShapeCoordinateEncoder.INSTANCE);
        MultiGeoShapeValues.GeoShapeValue value =  new MultiGeoShapeValues.GeoShapeValue(reader);

        List<Long> byteChangeHistory = new ArrayList<>();
        if (precision == 0) {
            new AllCellValues(null, tiler, byteChangeHistory::add);
        } else {
            GeoShapeCellValues values = new GeoShapeCellValues(null, precision, tiler, byteChangeHistory::add);
            tiler.setValues(values, value, precision);
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
            GeoShapeCellValues values = new GeoShapeCellValues(null, precision, tiler, circuitBreakerConsumer);
            tiler.setValues(values, value, precision);
            assertThat(values.getValuesBytes(), equalTo(curNumBytes));
            assertThat(limitedBreaker.getUsed(), equalTo(curNumBytes));
        });
    }
}
