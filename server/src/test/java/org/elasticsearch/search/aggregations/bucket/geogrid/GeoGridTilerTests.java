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
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoBoundingBoxTests;
import org.elasticsearch.common.geo.GeoRelation;
import org.elasticsearch.common.geo.GeoShapeCoordinateEncoder;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.TriangleTreeReader;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.fielddata.MultiGeoValues;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.geo.GeoTestUtils.encodeDecodeLat;
import static org.elasticsearch.common.geo.GeoTestUtils.encodeDecodeLon;
import static org.elasticsearch.common.geo.GeoTestUtils.triangleTreeReader;
import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.NORMALIZED_LATITUDE_MASK;
import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.NORMALIZED_NEGATIVE_LATITUDE_MASK;
import static org.hamcrest.Matchers.equalTo;

public class GeoGridTilerTests extends ESTestCase {
    private static final GeoGridTiler.GeoTileGridTiler GEOTILE = GeoGridTiler.GeoTileGridTiler.INSTANCE;
    private static final GeoGridTiler.GeoHashGridTiler GEOHASH = GeoGridTiler.GeoHashGridTiler.INSTANCE;

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
        MultiGeoValues.GeoShapeValue value =  new MultiGeoValues.GeoShapeValue(reader);

        // test shape within tile bounds
        {
            GeoShapeCellValues values = new GeoShapeCellValues(null, precision, GEOTILE);
            int count = GEOTILE.setValues(values, value, 13);
            assertThat(count, equalTo(1));
        }
        {
            GeoShapeCellValues values = new GeoShapeCellValues(null, precision, GEOTILE);
            int count = GEOTILE.setValues(values, value, 14);
            assertThat(count, equalTo(4));
        }
        {
            GeoShapeCellValues values = new GeoShapeCellValues(null, precision, GEOTILE);
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

    private void checkGeoTileSetValuesBruteAndRecursive(Geometry geometry) throws Exception {
        int precision = randomIntBetween(1, 5);
        GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
        geometry = indexer.prepareForIndexing(geometry);
        TriangleTreeReader reader = triangleTreeReader(geometry, GeoShapeCoordinateEncoder.INSTANCE);
        MultiGeoValues.GeoShapeValue value = new MultiGeoValues.GeoShapeValue(reader);
        GeoShapeCellValues recursiveValues = new GeoShapeCellValues(null, precision, GEOTILE);
        int recursiveCount;
        {
            recursiveCount = GEOTILE.setValuesByRasterization(0, 0, 0, recursiveValues, 0, precision, value);
        }
        GeoShapeCellValues bruteForceValues = new GeoShapeCellValues(null, precision, GEOTILE);
        int bruteForceCount;
        {
            final double tiles = 1 << precision;
            MultiGeoValues.BoundingBox bounds = value.boundingBox();
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
        MultiGeoValues.GeoShapeValue value = new MultiGeoValues.GeoShapeValue(reader);
        GeoShapeCellValues recursiveValues = new GeoShapeCellValues(null, precision, GEOHASH);
        int recursiveCount;
        {
            recursiveCount = GEOHASH.setValuesByRasterization("", recursiveValues, 0, precision, value);
        }
        GeoShapeCellValues bruteForceValues = new GeoShapeCellValues(null, precision, GEOHASH);
        int bruteForceCount;
        {
            MultiGeoValues.BoundingBox bounds = value.boundingBox();
            bruteForceCount = GEOHASH.setValuesByBruteForceScan(bruteForceValues, value, precision, bounds);
        }

        assertThat(geometry.toString(), recursiveCount, equalTo(bruteForceCount));

        long[] recursive = Arrays.copyOf(recursiveValues.getValues(), recursiveCount);
        long[] bruteForce = Arrays.copyOf(bruteForceValues.getValues(), bruteForceCount);
        Arrays.sort(recursive);
        Arrays.sort(bruteForce);
        assertArrayEquals(geometry.toString(), recursive, bruteForce);
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
            }, () -> boxToGeo(GeoBoundingBoxTests.randomBBox())));

            TriangleTreeReader reader = triangleTreeReader(geometry, GeoShapeCoordinateEncoder.INSTANCE);
            MultiGeoValues.GeoShapeValue value = new MultiGeoValues.GeoShapeValue(reader);
            GeoShapeCellValues unboundedCellValues = new GeoShapeCellValues(null, precision, GEOTILE);
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
            TriangleTreeReader reader = triangleTreeReader(point, GeoShapeCoordinateEncoder.INSTANCE);
            MultiGeoValues.GeoShapeValue value = new MultiGeoValues.GeoShapeValue(reader);
            GeoShapeCellValues unboundedCellValues = new GeoShapeCellValues(null, precision, GEOTILE);
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
        int precision = randomIntBetween(0, Geohash.PRECISION);
        assertThat(GEOHASH.encode(x, y, precision), equalTo(Geohash.longEncode(x, y, precision)));

        Rectangle tile = Geohash.toBoundingBox(Geohash.stringEncode(x, y, 5));

        Rectangle shapeRectangle = new Rectangle(tile.getMinX() + 0.00001, tile.getMaxX() - 0.00001,
            tile.getMaxY() - 0.00001,  tile.getMinY() + 0.00001);
        TriangleTreeReader reader = triangleTreeReader(shapeRectangle, GeoShapeCoordinateEncoder.INSTANCE);
        MultiGeoValues.GeoShapeValue value =  new MultiGeoValues.GeoShapeValue(reader);

        // test shape within tile bounds
        {
            GeoShapeCellValues values = new GeoShapeCellValues(null, precision, GEOTILE);
            int count = GEOHASH.setValues(values, value, 5);
            assertThat(count, equalTo(1));
        }
        {
            GeoShapeCellValues values = new GeoShapeCellValues(null, precision, GEOTILE);
            int count = GEOHASH.setValues(values, value, 6);
            assertThat(count, equalTo(32));
        }
        {
            GeoShapeCellValues values = new GeoShapeCellValues(null, precision, GEOTILE);
            int count = GEOHASH.setValues(values, value, 7);
            assertThat(count, equalTo(1024));
        }
    }

    private Geometry boxToGeo(GeoBoundingBox geoBox) {
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

    private int numTiles(MultiGeoValues.GeoValue geoValue, int precision) {
        MultiGeoValues.BoundingBox bounds = geoValue.boundingBox();
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
}
