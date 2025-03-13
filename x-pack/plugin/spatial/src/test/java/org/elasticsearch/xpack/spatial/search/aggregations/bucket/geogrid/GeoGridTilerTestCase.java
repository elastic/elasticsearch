/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.LongConsumer;

import static org.elasticsearch.xpack.spatial.util.GeoTestUtils.geoShapeValue;
import static org.elasticsearch.xpack.spatial.util.GeoTestUtils.randomBBox;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public abstract class GeoGridTilerTestCase<T extends GeoGridTiler> extends ESTestCase {

    protected static final LongConsumer NOOP_BREAKER = (l) -> {};

    protected final T getGridTiler(int precision) {
        final GeoBoundingBox bbox = randomBoolean()
            ? null
            : new GeoBoundingBox(new GeoPoint(Double.NaN, Double.NaN), new GeoPoint(Double.NaN, Double.NaN));
        return getGridTiler(bbox, precision);
    }

    protected abstract T getGridTiler(GeoBoundingBox bbox, int precision);

    protected abstract int maxPrecision();

    protected abstract Rectangle getCell(double lon, double lat, int precision);

    /** Tilers that are not rectangular cannot run all tests, eg. H3 tiler */
    protected boolean isRectangularTiler() {
        return true;
    }

    protected abstract long getCellsForDiffPrecision(int precisionDiff);

    protected abstract void assertSetValuesBruteAndRecursive(Geometry geometry) throws Exception;

    protected abstract int expectedBuckets(GeoShapeValues.GeoShapeValue value, int precision, GeoBoundingBox bbox) throws Exception;

    public void testMaxCellsBounded() {
        double lon = GeoTestUtil.nextLongitude();
        double lat = GeoTestUtil.nextLatitude();
        for (int i = 0; i < maxPrecision(); i++) {
            Rectangle tile = getCell(lon, lat, i);
            GeoBoundingBox boundingBox = new GeoBoundingBox(
                new GeoPoint(tile.getMaxLat(), tile.getMinLon()),
                new GeoPoint(tile.getMinLat(), tile.getMaxLon())
            );
            int otherPrecision = randomIntBetween(i, maxPrecision());
            T tiler = getGridTiler(boundingBox, otherPrecision);
            assertThat(tiler.getMaxCells(), greaterThanOrEqualTo(getCellsForDiffPrecision(otherPrecision - i)));
        }
    }

    public void testMaxCellsUnBounded() {
        for (int i = 0; i < maxPrecision(); i++) {
            T tiler = getGridTiler(i);
            assertThat(tiler.getMaxCells(), greaterThanOrEqualTo(getCellsForDiffPrecision(i)));
        }
    }

    public void testGeoGridSetValuesBruteAndRecursiveLine() throws Exception {
        Line geometry = GeometryTestUtils.randomLine(false);
        assertSetValuesBruteAndRecursive(geometry);

    }

    public void testGeoGridSetValuesBruteAndRecursiveMultiline() throws Exception {
        MultiLine geometry = GeometryTestUtils.randomMultiLine(false);
        assertSetValuesBruteAndRecursive(geometry);
    }

    public void testGeoGridSetValuesBruteAndRecursivePolygon() throws Exception {
        Geometry geometry = GeometryTestUtils.randomPolygon(false);
        assertSetValuesBruteAndRecursive(geometry);
    }

    public void testGeoGridSetValuesBruteAndRecursivePoint() throws Exception {
        Geometry geometry = GeometryTestUtils.randomPoint(false);
        assertSetValuesBruteAndRecursive(geometry);
    }

    public void testGeoGridSetValuesBruteAndRecursiveMultiPoint() throws Exception {
        Geometry geometry = GeometryTestUtils.randomMultiPoint(false);
        assertSetValuesBruteAndRecursive(geometry);
    }

    // tests that bounding boxes of shapes crossing the dateline are correctly wrapped
    public void testGeoGridSetValuesBoundingBoxes_BoundedGeoShapeCellValues() throws Exception {
        for (int i = 0; i < 10; i++) {
            int precision = randomIntBetween(0, 3);
            Geometry geometry = GeometryNormalizer.apply(
                Orientation.CCW,
                randomValueOtherThanMany(this::geometryIsInvalid, () -> boxToGeo(randomBBox()))
            );

            GeoBoundingBox geoBoundingBox = randomValueOtherThanMany(b -> b.right() == -180 && b.left() == 180, () -> randomBBox());
            GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);
            GeoShapeCellValues cellValues = new GeoShapeCellValues(
                makeGeoShapeValues(value),
                getGridTiler(geoBoundingBox, precision),
                NOOP_BREAKER
            );

            assertTrue(cellValues.advanceExact(0));
            int numBuckets = cellValues.docValueCount();
            int expected = expectedBuckets(value, precision, geoBoundingBox);
            assertThat("[" + i + ":" + precision + "] bucket count", numBuckets, equalTo(expected));
        }
    }

    // tests that bounding boxes that cross the dateline and cover all longitude values are correctly wrapped
    public void testGeoGridSetValuesBoundingBoxes_coversAllLongitudeValues() throws Exception {
        int precision = 3;
        Geometry geometry = new Rectangle(-92, 180, 0.99, -89);
        GeoBoundingBox geoBoundingBox = new GeoBoundingBox(new GeoPoint(5, 0.6), new GeoPoint(-5, 0.5));
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

    public void testGeoGridSetValuesBoundingBoxes_UnboundedGeoShapeCellValues() throws Exception {
        for (int i = 0; i < 100; i++) {
            int precision = randomIntBetween(0, 3);
            Geometry geometry = randomValueOtherThanMany(this::geometryIsInvalid, () -> boxToGeo(randomBBox()));
            GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);
            GeoShapeCellValues unboundedCellValues = new GeoShapeCellValues(
                makeGeoShapeValues(value),
                getGridTiler(precision),
                NOOP_BREAKER
            );

            assertTrue(unboundedCellValues.advanceExact(0));
            int numBuckets = unboundedCellValues.docValueCount();
            int expected = expectedBuckets(value, precision, null);
            assertThat("[" + i + ":" + precision + "] bucket count", numBuckets, equalTo(expected));
        }
    }

    public void testGeoTileShapeContainsBoundDateLine() throws Exception {
        Rectangle tile = new Rectangle(178, -178, 2, -2);
        Rectangle shapeRectangle = new Rectangle(170, -170, 10, -10);
        GeoShapeValues.GeoShapeValue value = geoShapeValue(shapeRectangle);

        GeoBoundingBox boundingBox = new GeoBoundingBox(
            new GeoPoint(tile.getMaxLat(), tile.getMinLon()),
            new GeoPoint(tile.getMinLat(), tile.getMaxLon())
        );

        GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), getGridTiler(boundingBox, 4), NOOP_BREAKER);
        assertTrue(values.advanceExact(0));
        int numTiles = values.docValueCount();
        int expectedTiles = expectedBuckets(value, 4, boundingBox);
        assertThat(expectedTiles, equalTo(numTiles));
    }

    public void testBoundsExcludeTouchingTiles() throws Exception {
        final int precision = randomIntBetween(4, maxPrecision()) - 4;
        assumeTrue("Test only works for rectangular tilers", isRectangularTiler());

        final Rectangle rectangle = getCell(GeoTestUtil.nextLongitude(), GeoTestUtil.nextLatitude(), precision);
        final GeoBoundingBox box = new GeoBoundingBox(
            new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
            new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
        );
        final Rectangle other = new Rectangle(
            Math.max(-180, rectangle.getMinX() - 1),
            Math.min(180, rectangle.getMaxX() + 1),
            Math.min(90, rectangle.getMaxY() + 1),
            Math.max(-90, rectangle.getMinY() - 1)
        );
        final GeoShapeValues.GeoShapeValue value = geoShapeValue(other);
        for (int i = 0; i < 4; i++) {
            final GeoGridTiler bounded = getGridTiler(box, precision + i);
            final GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), bounded, NOOP_BREAKER);
            assertTrue(values.advanceExact(0));
            final int numTiles = values.docValueCount();
            final int expected = (int) getCellsForDiffPrecision(i);
            assertThat("For precision " + (precision + i), numTiles, equalTo(expected));
        }
    }

    public void testGridCircuitBreaker() throws IOException {
        T tiler = getGridTiler(randomIntBetween(0, 3));
        Geometry geometry = GeometryTestUtils.randomPolygon(false);

        GeoShapeValues.GeoShapeValue value = geoShapeValue(geometry);

        List<Long> byteChangeHistory = new ArrayList<>();
        {
            GeoShapeCellValues values = new GeoShapeCellValues(null, tiler, byteChangeHistory::add);
            tiler.setValues(values, value);
        }

        final long maxNumBytes;
        final long curNumBytes;
        if (byteChangeHistory.size() == 1) {
            curNumBytes = maxNumBytes = byteChangeHistory.get(0);
        } else {
            long oldNumBytes = -byteChangeHistory.get(byteChangeHistory.size() - 1);
            curNumBytes = byteChangeHistory.get(byteChangeHistory.size() - 2);
            maxNumBytes = oldNumBytes + curNumBytes;
        }

        CircuitBreakerService service = new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            Settings.EMPTY,
            Collections.singletonList(new BreakerSettings("limited", maxNumBytes - 1, 1.0)),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        CircuitBreaker limitedBreaker = service.getBreaker("limited");

        LongConsumer circuitBreakerConsumer = (l) -> limitedBreaker.addEstimateBytesAndMaybeBreak(l, "agg");
        expectThrows(CircuitBreakingException.class, () -> {
            GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), tiler, circuitBreakerConsumer);
            assertTrue(values.advanceExact(0));
            assertThat((long) values.getValues().length * Long.BYTES, equalTo(curNumBytes));
            assertThat(limitedBreaker.getUsed(), equalTo(curNumBytes));
        });
    }

    protected boolean geometryIsInvalid(Geometry g) {
        try {
            // make sure is a valid shape
            new GeoShapeIndexer(Orientation.CCW, "test").indexShape(g);
            return false;
        } catch (Exception e) {
            return true;
        }
    }

    protected GeoShapeValues makeGeoShapeValues(GeoShapeValues.GeoShapeValue... values) {
        return new GeoShapeValues() {
            int index = 0;

            @Override
            public boolean advanceExact(int doc) {
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

    private static Geometry boxToGeo(GeoBoundingBox geoBox) {
        // turn into polygon
        if (geoBox.right() < geoBox.left() && geoBox.right() != -180) {
            return new MultiPolygon(
                List.of(
                    new Polygon(
                        new LinearRing(
                            new double[] { -180, geoBox.right(), geoBox.right(), -180, -180 },
                            new double[] { geoBox.bottom(), geoBox.bottom(), geoBox.top(), geoBox.top(), geoBox.bottom() }
                        )
                    ),
                    new Polygon(
                        new LinearRing(
                            new double[] { geoBox.left(), 180, 180, geoBox.left(), geoBox.left() },
                            new double[] { geoBox.bottom(), geoBox.bottom(), geoBox.top(), geoBox.top(), geoBox.bottom() }
                        )
                    )
                )
            );
        } else {
            double right = GeoUtils.normalizeLon(geoBox.right());
            return new Polygon(
                new LinearRing(
                    new double[] { geoBox.left(), right, right, geoBox.left(), geoBox.left() },
                    new double[] { geoBox.bottom(), geoBox.bottom(), geoBox.top(), geoBox.top(), geoBox.bottom() }
                )
            );
        }
    }
}
