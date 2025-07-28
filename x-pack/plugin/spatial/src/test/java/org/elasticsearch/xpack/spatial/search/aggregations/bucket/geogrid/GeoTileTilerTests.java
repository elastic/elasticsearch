/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileBoundedPredicate;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.query.GeoGridQueryBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.LATITUDE_MASK;
import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.longEncodeTiles;
import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.stringEncode;
import static org.elasticsearch.xpack.spatial.util.GeoTestUtils.encodeDecodeLat;
import static org.elasticsearch.xpack.spatial.util.GeoTestUtils.encodeDecodeLon;
import static org.elasticsearch.xpack.spatial.util.GeoTestUtils.geoShapeValue;
import static org.hamcrest.Matchers.equalTo;

public class GeoTileTilerTests extends GeoGridTilerTestCase<GeoTileGridTiler> {

    @Override
    protected GeoTileGridTiler getGridTiler(GeoBoundingBox bbox, int precision) {
        return GeoTileGridTiler.makeGridTiler(precision, bbox);
    }

    @Override
    protected Rectangle getCell(double lon, double lat, int precision) {
        return GeoTileUtils.toBoundingBox(GeoTileUtils.longEncode(lon, lat, precision));
    }

    @Override
    protected int maxPrecision() {
        return GeoTileUtils.MAX_ZOOM;
    }

    @Override
    protected long getCellsForDiffPrecision(int precisionDiff) {
        return (1L << precisionDiff) * (1L << precisionDiff);
    }

    @Override
    protected void assertSetValuesBruteAndRecursive(Geometry geometry) throws Exception {
        int precision = randomIntBetween(1, 4);
        GeoTileGridTiler tiler = getGridTiler(precision);
        geometry = GeometryNormalizer.apply(Orientation.CCW, geometry);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);
        GeoShapeCellValues recursiveValues = new GeoShapeCellValues(null, tiler, NOOP_BREAKER);
        int recursiveCount;
        {
            recursiveCount = tiler.setValuesByRasterization(0, 0, 0, recursiveValues, 0, value);
        }
        GeoShapeCellValues bruteForceValues = new GeoShapeCellValues(null, tiler, NOOP_BREAKER);
        int bruteForceCount;
        {
            final int tiles = 1 << precision;
            GeoShapeValues.BoundingBox bounds = value.boundingBox();
            int minXTile = GeoTileUtils.getXTile(bounds.minX(), tiles);
            int minYTile = GeoTileUtils.getYTile(bounds.maxY(), tiles);
            int maxXTile = GeoTileUtils.getXTile(bounds.maxX(), tiles);
            int maxYTile = GeoTileUtils.getYTile(bounds.minY(), tiles);
            bruteForceCount = tiler.setValuesByBruteForceScan(bruteForceValues, value, minXTile, minYTile, maxXTile, maxYTile);
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
        int count = 0;

        if (bounds.bottom > GeoTileUtils.NORMALIZED_LATITUDE_MASK || bounds.top < GeoTileUtils.NORMALIZED_NEGATIVE_LATITUDE_MASK) {
            return 0;
        }

        if (bbox != null) {
            if (bbox.bottom() > GeoTileUtils.NORMALIZED_LATITUDE_MASK || bbox.top() < GeoTileUtils.NORMALIZED_NEGATIVE_LATITUDE_MASK) {
                return 0;
            }
        }

        if (precision == 0) {
            return 1;
        }

        final int tiles = 1 << precision;
        int minYTile = GeoTileUtils.getYTile(bounds.maxY(), tiles);
        int maxYTile = GeoTileUtils.getYTile(bounds.minY(), tiles);
        if ((bounds.posLeft >= 0 && bounds.posRight >= 0) && (bounds.negLeft < 0 && bounds.negRight < 0)) {
            // box one
            int minXTileNeg = GeoTileUtils.getXTile(bounds.negLeft, tiles);
            int maxXTileNeg = GeoTileUtils.getXTile(bounds.negRight, tiles);

            for (int x = minXTileNeg; x <= maxXTileNeg; x++) {
                for (int y = minYTile; y <= maxYTile; y++) {
                    Rectangle r = GeoTileUtils.toBoundingBox(x, y, precision);
                    if (tileIntersectsBounds(x, y, precision, bbox) && intersects(x, y, precision, geoValue)) {
                        count += 1;
                    }
                }
            }

            // box two
            int minXTilePos = GeoTileUtils.getXTile(bounds.posLeft, tiles);
            if (minXTilePos > maxXTileNeg + 1) {
                minXTilePos -= 1;
            }

            int maxXTilePos = GeoTileUtils.getXTile(bounds.posRight, tiles);

            for (int x = minXTilePos; x <= maxXTilePos; x++) {
                for (int y = minYTile; y <= maxYTile; y++) {
                    if (tileIntersectsBounds(x, y, precision, bbox) && intersects(x, y, precision, geoValue)) {
                        count += 1;
                    }
                }
            }
            return count;
        } else {
            int minXTile = GeoTileUtils.getXTile(bounds.minX(), tiles);
            int maxXTile = GeoTileUtils.getXTile(bounds.maxX(), tiles);

            if (minXTile == maxXTile && minYTile == maxYTile) {
                return tileIntersectsBounds(minXTile, minYTile, precision, bbox) ? 1 : 0;
            }

            for (int x = minXTile; x <= maxXTile; x++) {
                for (int y = minYTile; y <= maxYTile; y++) {
                    Rectangle r = GeoTileUtils.toBoundingBox(x, y, precision);
                    if (tileIntersectsBounds(x, y, precision, bbox) && intersects(x, y, precision, geoValue)) {
                        count += 1;
                    }
                }
            }
            return count;
        }
    }

    private boolean intersects(int x, int y, int precision, GeoShapeValues.GeoShapeValue geoValue) throws IOException {
        Rectangle r = GeoGridQueryBuilder.getQueryTile(stringEncode(longEncodeTiles(precision, x, y)));
        return geoValue.relate(
            GeoEncodingUtils.encodeLongitude(r.getMinLon()),
            GeoEncodingUtils.encodeLongitude(r.getMaxLon()),
            GeoEncodingUtils.encodeLatitude(r.getMinLat()),
            GeoEncodingUtils.encodeLatitude(r.getMaxLat())
        ) != GeoRelation.QUERY_DISJOINT;
    }

    private boolean tileIntersectsBounds(int x, int y, int precision, GeoBoundingBox bbox) {
        if (bbox == null) {
            return true;
        }
        GeoTileBoundedPredicate predicate = new GeoTileBoundedPredicate(precision, bbox);
        return predicate.validTile(x, y, precision);
    }

    public void testGeoTile() throws Exception {
        double x = randomDouble();
        double y = randomDouble();
        int precision = randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
        assertThat(getGridTiler(precision).encode(x, y), equalTo(GeoTileUtils.longEncode(x, y, precision)));

        // create rectangle within tile and check bound counts
        Rectangle tile = GeoTileUtils.toBoundingBox(1309, 3166, 13);
        Rectangle shapeRectangle = new Rectangle(
            tile.getMinX() + 0.00001,
            tile.getMaxX() - 0.00001,
            tile.getMaxY() - 0.00001,
            tile.getMinY() + 0.00001
        );
        GeoShapeValues.GeoShapeValue value = geoShapeValue(shapeRectangle);
        // test shape within tile bounds
        {
            GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), getGridTiler(13), NOOP_BREAKER);
            assertTrue(values.advanceExact(0));
            assertThat(values.docValueCount(), equalTo(1));
        }
        {
            GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), getGridTiler(14), NOOP_BREAKER);
            assertTrue(values.advanceExact(0));
            assertThat(values.docValueCount(), equalTo(4));
        }
        {
            GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), getGridTiler(15), NOOP_BREAKER);
            assertTrue(values.advanceExact(0));
            assertThat(values.docValueCount(), equalTo(16));
        }
    }

    public void testMaxCellsBoundedWithAnotherCell() {
        double lon = GeoTestUtil.nextLongitude();
        double lat = GeoTestUtil.nextLatitude();
        for (int i = 0; i < maxPrecision(); i++) {
            Rectangle tile = getCell(lon, lat, i);
            GeoBoundingBox boundingBox = new GeoBoundingBox(
                new GeoPoint(tile.getMaxLat(), tile.getMinLon()),
                new GeoPoint(tile.getMinLat(), tile.getMaxLon())
            );
            int otherPrecision = randomIntBetween(i, maxPrecision());
            GeoGridTiler tiler = getGridTiler(boundingBox, otherPrecision);
            assertThat(tiler.getMaxCells(), equalTo(getCellsForDiffPrecision(otherPrecision - i)));
        }
    }

    public void testBoundGridOutOfRange() throws Exception {
        GeoBoundingBox boundingBox = new GeoBoundingBox(new GeoPoint(90, -180), new GeoPoint(89, 180));
        double lon = GeoTestUtil.nextLongitude();
        double lat = GeoTestUtil.nextLatitude();
        GeoShapeValues.GeoShapeValue value = geoShapeValue(new Point(lon, lat));
        for (int i = 0; i < maxPrecision(); i++) {
            GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), getGridTiler(boundingBox, i), NOOP_BREAKER);
            assertTrue(values.advanceExact(0));
            int numTiles = values.docValueCount();
            assertThat(numTiles, equalTo(0));
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
            new Point((bbox.getMinX() + bbox.getMaxX()) / 2, bbox.getMaxY()), };

        for (Point point : pointCorners) {
            if (point.getX() == GeoUtils.MAX_LON || point.getY() == -LATITUDE_MASK) {
                continue;
            }
            GeoShapeValues.GeoShapeValue value = geoShapeValue(point);
            GeoShapeCellValues unboundedCellValues = new GeoShapeCellValues(
                makeGeoShapeValues(value),
                getGridTiler(precision),
                NOOP_BREAKER
            );
            assertTrue(unboundedCellValues.advanceExact(0));
            int numTiles = unboundedCellValues.docValueCount();
            assertThat(numTiles, equalTo(1));
            long tilerHash = unboundedCellValues.getValues()[0];
            long pointHash = GeoTileUtils.longEncode(encodeDecodeLon(point.getX()), encodeDecodeLat(point.getY()), precision);
            assertThat(tilerHash, equalTo(pointHash));
        }
    }

    public void testMultiPointOutOfBounds() throws Exception {
        final double maxLat = randomDoubleBetween(GeoTileUtils.NORMALIZED_LATITUDE_MASK, 90, false);
        final double minLat = randomDoubleBetween(-90, Math.nextDown(GeoTileUtils.NORMALIZED_NEGATIVE_LATITUDE_MASK), true);
        // points are out of bounds, should not generate any bucket
        MultiPoint points = new MultiPoint(List.of(new Point(0, maxLat), new Point(0, minLat)));
        final GeoShapeValues.GeoShapeValue value = geoShapeValue(points);
        final GeoGridTiler tiler = getGridTiler(0);
        final GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), tiler, NOOP_BREAKER);
        assertTrue(values.advanceExact(0));
        assertThat(values.docValueCount(), equalTo(0));
    }
}
