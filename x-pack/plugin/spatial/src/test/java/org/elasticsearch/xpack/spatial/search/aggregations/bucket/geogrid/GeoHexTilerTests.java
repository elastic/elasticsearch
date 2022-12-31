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
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.h3.H3;
import org.elasticsearch.xpack.spatial.common.H3CartesianUtil;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.xpack.spatial.util.GeoTestUtils.geoShapeValue;
import static org.hamcrest.Matchers.equalTo;

public class GeoHexTilerTests extends GeoGridTilerTestCase {
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
        return H3CartesianUtil.toBoundingBox(H3.geoToH3(lat, lon, precision));
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

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/92541")
    public void testGeoGridSetValuesBoundingBoxes_BoundedGeoShapeCellValues() throws Exception {
        super.testGeoGridSetValuesBoundingBoxes_BoundedGeoShapeCellValues();
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/92562")
    public void testGeoGridSetValuesBoundingBoxes_UnboundedGeoShapeCellValues() throws Exception {
        super.testGeoGridSetValuesBoundingBoxes_UnboundedGeoShapeCellValues();
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
                getBoundedGridTiler(boundingBox, precision),
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
                getBoundedGridTiler(boundingBox, precision),
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

    private void assertCorner(long[] h3bins, Point point, int precision, String msg) throws IOException {
        GeoShapeValues.GeoShapeValue cornerValue = geoShapeValue(point);
        GeoShapeCellValues cornerValues = new GeoShapeCellValues(
            makeGeoShapeValues(cornerValue),
            getUnboundedGridTiler(precision),
            NOOP_BREAKER
        );
        assertTrue(cornerValues.advanceExact(0));
        long[] h3binsCorner = ArrayUtil.copyOfSubArray(cornerValues.getValues(), 0, cornerValues.docValueCount());
        for (long corner : h3binsCorner) {
            assertTrue(msg, Arrays.binarySearch(h3bins, corner) != -1);
        }
    }

    @Override
    protected void assertSetValuesBruteAndRecursive(Geometry geometry) throws Exception {
        int precision = randomIntBetween(1, 4);
        UnboundedGeoHexGridTiler tiler = new UnboundedGeoHexGridTiler(precision);
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
        AbstractGeoHexGridTiler tiler,
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
        return computeBuckets(H3.getLongRes0Cells(), bbox, geoValue, precision);
    }

    private int computeBuckets(long[] children, GeoBoundingBox bbox, GeoShapeValues.GeoShapeValue geoValue, int finalPrecision)
        throws IOException {
        int count = 0;
        for (long child : children) {
            if (H3.getResolution(child) == finalPrecision) {
                if (intersects(child, geoValue, bbox, finalPrecision)) {
                    count++;
                }
            } else {
                count += computeBuckets(H3.h3ToChildren(child), bbox, geoValue, finalPrecision);
            }
        }
        return count;
    }

    private boolean intersects(long h3, GeoShapeValues.GeoShapeValue geoValue, GeoBoundingBox bbox, int finalPrecision) throws IOException {
        if (addressIntersectsBounds(h3, bbox, finalPrecision) == false) {
            return false;
        }
        UnboundedGeoHexGridTiler predicate = new UnboundedGeoHexGridTiler(finalPrecision);
        return predicate.relateTile(geoValue, h3) != GeoRelation.QUERY_DISJOINT;
    }

    private boolean addressIntersectsBounds(long h3, GeoBoundingBox bbox, int finalPrecision) {
        if (bbox == null) {
            return true;
        }
        BoundedGeoHexGridTiler predicate = new BoundedGeoHexGridTiler(finalPrecision, bbox);
        return predicate.h3IntersectsBounds(h3);
    }
}
