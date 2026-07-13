/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile.feature;

import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FeatureFactoriesConsistencyTests extends ESTestCase {

    public void testPoint() throws IOException {
        int z = randomIntBetween(1, 10);
        int x = randomIntBetween(0, (1 << z) - 1);
        int y = randomIntBetween(0, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        int padPixels = randomIntBetween(0, extent);
        Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, z);
        FeatureFactory factory = new FeatureFactory(z, x, y, extent, padPixels);
        List<Point> points = new ArrayList<>();
        List<GeoPoint> geoPoints = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            double lat = randomValueOtherThanMany((l) -> rectangle.getMinY() > l || rectangle.getMaxY() < l, GeoTestUtil::nextLatitude);
            double lon = randomValueOtherThanMany((l) -> rectangle.getMinX() > l || rectangle.getMaxX() < l, GeoTestUtil::nextLongitude);
            byte[] b1 = factory.point(lon, lat);
            Point point = new Point(lon, lat);
            byte[] b2 = factory.getFeatures(point).get(0);
            assertArrayEquals(b1, b2);
            points.add(point);
            geoPoints.add(new GeoPoint(lat, lon));
        }
        byte[] b1 = factory.points(geoPoints);
        byte[] b2 = factory.getFeatures(new MultiPoint(points)).get(0);
        assertArrayEquals(b1, b2);
    }

    public void testRectangle() throws IOException {
        int z = randomIntBetween(1, 10);
        int x = randomIntBetween(0, (1 << z) - 1);
        int y = randomIntBetween(0, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        int padPixels = randomIntBetween(0, extent);
        FeatureFactory factory = new FeatureFactory(z, x, y, extent, padPixels);
        Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);
        for (int i = 0; i < extent; i++) {
            byte[] b1 = factory.box(r.getMinLon(), r.getMaxLon(), r.getMinLat(), r.getMaxLat());
            byte[] b2 = factory.getFeatures(r).get(0);
            assertArrayEquals(extent + "", b1, b2);
        }
    }

    public void testDegeneratedRectangle() throws IOException {
        int z = randomIntBetween(3, 10);
        int x = randomIntBetween(1, (1 << z) - 1);
        int y = randomIntBetween(1, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        int padPixels = randomIntBetween(0, extent);
        FeatureFactory factory = new FeatureFactory(z, x, y, extent, padPixels);
        {
            Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);
            // box is a point
            byte[] b1 = factory.box(r.getMaxLon(), r.getMaxLon(), r.getMaxLat(), r.getMaxLat());
            byte[] b2 = factory.getFeatures(new Rectangle(r.getMaxLon(), r.getMaxLon(), r.getMaxLat(), r.getMaxLat())).get(0);
            assertArrayEquals(extent + "", b1, b2);
        }
        {
            Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);
            // box is a line
            byte[] b1 = factory.box(r.getMinLon(), r.getMinLon(), r.getMinLat(), r.getMaxLat());
            byte[] b2 = factory.getFeatures(new Rectangle(r.getMinLon(), r.getMinLon(), r.getMaxLat(), r.getMinLat())).get(0);
            assertArrayEquals(extent + "", b1, b2);
        }
    }
}
