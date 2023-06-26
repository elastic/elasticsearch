/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashBoundedPredicate;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.query.GeoGridQueryBuilder;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.xpack.spatial.util.GeoTestUtils.geoShapeValue;
import static org.hamcrest.Matchers.equalTo;

public class GeoHashTilerTests extends GeoGridTilerTestCase<GeoHashGridTiler> {

    @Override
    protected GeoHashGridTiler getGridTiler(GeoBoundingBox bbox, int precision) {
        return GeoHashGridTiler.makeGridTiler(precision, bbox);
    }

    @Override
    protected int maxPrecision() {
        return Geohash.PRECISION;
    }

    @Override
    protected Rectangle getCell(double lon, double lat, int precision) {
        if (precision == 0) {
            return new Rectangle(-180, 180, 90, -90);
        }
        final String hash = Geohash.stringEncode(lon, lat, precision);
        return Geohash.toBoundingBox(hash);
    }

    @Override
    protected long getCellsForDiffPrecision(int precisionDiff) {
        return (long) Math.pow(32, precisionDiff);
    }

    @Override
    protected void assertSetValuesBruteAndRecursive(Geometry geometry) throws Exception {
        int precision = randomIntBetween(1, 3);
        GeoHashGridTiler tiler = getGridTiler(precision);
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

    @Override
    protected int expectedBuckets(GeoShapeValues.GeoShapeValue geoValue, int precision, GeoBoundingBox bbox) throws Exception {
        if (precision == 0) {
            return 1;
        }
        GeoShapeValues.BoundingBox bounds = geoValue.boundingBox();
        if (bounds.minX() == bounds.maxX() && bounds.minY() == bounds.maxY()) {
            String hash = Geohash.stringEncode(bounds.minX(), bounds.minY(), precision);
            if (hashIntersectsBounds(hash, bbox) && intersects(hash, geoValue)) {
                return 1;
            }
            return 0;
        }
        return computeBuckets("", bbox, geoValue, precision);
    }

    private int computeBuckets(String hash, GeoBoundingBox bbox, GeoShapeValues.GeoShapeValue geoValue, int finalPrecision)
        throws IOException {
        int count = 0;
        String[] hashes = Geohash.getSubGeohashes(hash);
        for (int i = 0; i < hashes.length; i++) {
            if (hashIntersectsBounds(hashes[i], bbox) == false) {
                continue;
            }
            if (intersects(hashes[i], geoValue)) {
                if (hashes[i].length() == finalPrecision) {
                    count++;
                } else {
                    count += computeBuckets(hashes[i], bbox, geoValue, finalPrecision);
                }
            }
        }
        return count;
    }

    private boolean intersects(String hash, GeoShapeValues.GeoShapeValue geoValue) throws IOException {
        final Rectangle r = GeoGridQueryBuilder.getQueryHash(hash);
        return geoValue.relate(
            GeoEncodingUtils.encodeLongitude(r.getMinLon()),
            GeoEncodingUtils.encodeLongitude(r.getMaxLon()),
            GeoEncodingUtils.encodeLatitude(r.getMinLat()),
            GeoEncodingUtils.encodeLatitude(r.getMaxLat())
        ) != GeoRelation.QUERY_DISJOINT;
    }

    private boolean hashIntersectsBounds(String hash, GeoBoundingBox bbox) {
        if (bbox == null) {
            return true;
        }
        GeoHashBoundedPredicate predicate = new GeoHashBoundedPredicate(hash.length(), bbox);
        return predicate.validHash(hash);
    }

    public void testGeoHash() throws Exception {
        double x = randomDouble();
        double y = randomDouble();
        int precision = randomIntBetween(0, 6);
        assertThat(getGridTiler(precision).encode(x, y), equalTo(Geohash.longEncode(x, y, precision)));

        Rectangle tile = Geohash.toBoundingBox(Geohash.stringEncode(x, y, 5));

        Rectangle shapeRectangle = new Rectangle(
            tile.getMinX() + 0.00001,
            tile.getMaxX() - 0.00001,
            tile.getMaxY() - 0.00001,
            tile.getMinY() + 0.00001
        );
        GeoShapeValues.GeoShapeValue value = geoShapeValue(shapeRectangle);

        // test shape within tile bounds
        {
            GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), getGridTiler(5), NOOP_BREAKER);
            assertTrue(values.advanceExact(0));
            int count = values.docValueCount();
            assertThat(count, equalTo(1));
        }
        {
            GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), getGridTiler(6), NOOP_BREAKER);
            assertTrue(values.advanceExact(0));
            int count = values.docValueCount();
            assertThat(count, equalTo(32));
        }
        {
            GeoShapeCellValues values = new GeoShapeCellValues(makeGeoShapeValues(value), getGridTiler(7), NOOP_BREAKER);
            assertTrue(values.advanceExact(0));
            int count = values.docValueCount();
            assertThat(count, equalTo(1024));
        }
    }
}
