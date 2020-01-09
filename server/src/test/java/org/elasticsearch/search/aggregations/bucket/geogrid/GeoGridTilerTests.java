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

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.geo.GeoShapeCoordinateEncoder;
import org.elasticsearch.common.geo.TriangleTreeReader;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.fielddata.MultiGeoValues;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.common.geo.GeoTestUtils.triangleTreeReader;
import static org.hamcrest.Matchers.equalTo;

public class GeoGridTilerTests extends ESTestCase {
    private static final GeoGridTiler.GeoTileGridTiler GEOTILE = GeoGridTiler.GeoTileGridTiler.INSTANCE;
    private static final GeoGridTiler.GeoHashGridTiler GEOHASH = GeoGridTiler.GeoHashGridTiler.INSTANCE;
    private static final CircuitBreaker NOOP_BREAKER = new NoopCircuitBreaker("noop-test");

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
            CellIdSource.GeoShapeCellValues values = new CellIdSource.GeoShapeCellValues(null, precision, GEOTILE, NOOP_BREAKER);
            int count = GEOTILE.setValues(values, value, 13);
            assertThat(count, equalTo(1));
        }
        {
            CellIdSource.GeoShapeCellValues values = new CellIdSource.GeoShapeCellValues(null, precision, GEOTILE, NOOP_BREAKER);
            int count = GEOTILE.setValues(values, value, 14);
            assertThat(count, equalTo(4));
        }
        {
            CellIdSource.GeoShapeCellValues values = new CellIdSource.GeoShapeCellValues(null, precision, GEOTILE, NOOP_BREAKER);
            int count = GEOTILE.setValues(values, value, 15);
            assertThat(count, equalTo(16));
        }
    }

    public void testGeoTileSetValuesBruteAndRecursiveMultiline() throws Exception {
        MultiLine geometry = GeometryTestUtils.randomMultiLine(false);
        checkGeoTileSetValuesBruteAndRecursive(geometry);
    }

    public void testGeoTileSetValuesBruteAndRecursivePolygon() throws Exception {
        Geometry geometry = GeometryTestUtils.randomPolygon(false);
        checkGeoTileSetValuesBruteAndRecursive(geometry);
    }

    public void testGeoTileSetValuesBruteAndRecursivePoints() throws Exception {
        Geometry geometry = randomBoolean() ? GeometryTestUtils.randomPoint(false) : GeometryTestUtils.randomMultiPoint(false);
        checkGeoTileSetValuesBruteAndRecursive(geometry);
    }

    private void checkGeoTileSetValuesBruteAndRecursive(Geometry geometry) throws Exception {
        int precision = randomIntBetween(1, 10);
        GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
        geometry = indexer.prepareForIndexing(geometry);
        TriangleTreeReader reader = triangleTreeReader(geometry, GeoShapeCoordinateEncoder.INSTANCE);
        MultiGeoValues.GeoShapeValue value = new MultiGeoValues.GeoShapeValue(reader);
        CellIdSource.GeoShapeCellValues recursiveValues = new CellIdSource.GeoShapeCellValues(null, precision, GEOTILE, NOOP_BREAKER);
        int recursiveCount;
        {
            recursiveCount = GEOTILE.setValuesByRasterization(0, 0, 0, recursiveValues, 0,
                                                              precision, value, value.boundingBox());
        }
        CellIdSource.GeoShapeCellValues bruteForceValues = new CellIdSource.GeoShapeCellValues(null, precision, GEOTILE, NOOP_BREAKER);
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
            CellIdSource.GeoShapeCellValues values = new CellIdSource.GeoShapeCellValues(null, precision, GEOTILE, NOOP_BREAKER);
            int count = GEOHASH.setValues(values, value, 5);
            assertThat(count, equalTo(1));
        }
        {
            CellIdSource.GeoShapeCellValues values = new CellIdSource.GeoShapeCellValues(null, precision, GEOTILE, NOOP_BREAKER);
            int count = GEOHASH.setValues(values, value, 6);
            assertThat(count, equalTo(32));
        }
        {
            CellIdSource.GeoShapeCellValues values = new CellIdSource.GeoShapeCellValues(null, precision, GEOTILE, NOOP_BREAKER);
            int count = GEOHASH.setValues(values, value, 7);
            assertThat(count, equalTo(1024));
        }
    }

    public void testGeoHashMaxBuckets() throws IOException {
        MultiPoint multiPoint = GeometryTestUtils.randomMultiPoint(false);
        int precision = randomIntBetween(0, 10);
        TriangleTreeReader reader = triangleTreeReader(multiPoint, GeoShapeCoordinateEncoder.INSTANCE);
        MultiGeoValues.GeoShapeValue value =  new MultiGeoValues.GeoShapeValue(reader);


        final int numBytes;
        {
            CellIdSource.GeoShapeCellValues values = new CellIdSource.GeoShapeCellValues(null, precision, GEOHASH, NOOP_BREAKER);
            GEOHASH.setValues(values, value, precision);
            numBytes = values.getValues().length * Long.BYTES;
        }

        CircuitBreakerService service = new HierarchyCircuitBreakerService(Settings.EMPTY, new ClusterSettings(Settings.EMPTY,
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        service.registerBreaker(new BreakerSettings("limited", numBytes - 1, 1.0));
        CircuitBreaker limitedBreaker = service.getBreaker("limited");

        CellIdSource.GeoShapeCellValues values = new CellIdSource.GeoShapeCellValues(null, precision, GEOHASH, limitedBreaker);
        expectThrows(CircuitBreakingException.class, () -> GEOHASH.setValues(values, value, precision));
    }

    public void testGeoTileMaxBuckets() throws IOException {
        MultiPoint multiPoint = GeometryTestUtils.randomMultiPoint(false);
        int precision = randomIntBetween(0, 10);
        TriangleTreeReader reader = triangleTreeReader(multiPoint, GeoShapeCoordinateEncoder.INSTANCE);
        MultiGeoValues.GeoShapeValue value =  new MultiGeoValues.GeoShapeValue(reader);

        final int numBytes;
        {
            CellIdSource.GeoShapeCellValues values = new CellIdSource.GeoShapeCellValues(null, precision, GEOTILE, NOOP_BREAKER);
            GEOTILE.setValues(values, value, precision);
            numBytes = values.getValues().length * Long.BYTES;
        }

        CircuitBreakerService service = new HierarchyCircuitBreakerService(Settings.EMPTY, new ClusterSettings(Settings.EMPTY,
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        service.registerBreaker(new BreakerSettings("limited", numBytes - 1, 1.0));
        CircuitBreaker limitedBreaker = service.getBreaker("limited");

        CellIdSource.GeoShapeCellValues values = new CellIdSource.GeoShapeCellValues(null, precision, GEOTILE, limitedBreaker);
        expectThrows(CircuitBreakingException.class, () -> GEOTILE.setValues(values, value, precision));
    }
}
