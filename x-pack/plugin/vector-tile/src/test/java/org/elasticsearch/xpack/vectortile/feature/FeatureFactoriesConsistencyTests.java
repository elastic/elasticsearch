/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile.feature;

import org.apache.lucene.geo.GeoTestUtil;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.SimpleFeatureFactory;
import org.elasticsearch.common.geo.SphericalMercatorUtils;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FeatureFactoriesConsistencyTests extends ESTestCase {

    public void testPoint() throws IOException {
        int z = randomIntBetween(1, 10);
        int x = randomIntBetween(0, (1 << z) - 1);
        int y = randomIntBetween(0, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        // check if we might have numerical error due to floating point arithmetic
        assumeFalse("", hasNumericalError(z, x, y, extent));
        Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, z);
        SimpleFeatureFactory builder = new SimpleFeatureFactory(z, x, y, extent);
        FeatureFactory factory = new FeatureFactory(z, x, y, extent);
        List<Point> points = new ArrayList<>();
        List<GeoPoint> geoPoints = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            double lat = randomValueOtherThanMany((l) -> rectangle.getMinY() > l || rectangle.getMaxY() < l, GeoTestUtil::nextLatitude);
            double lon = randomValueOtherThanMany((l) -> rectangle.getMinX() > l || rectangle.getMaxX() < l, GeoTestUtil::nextLongitude);
            byte[] b1 = builder.point(lon, lat);
            Point point = new Point(lon, lat);
            byte[] b2 = factory.getFeatures(point).get(0);
            assertArrayEquals(b1, b2);
            points.add(point);
            geoPoints.add(new GeoPoint(lat, lon));
        }
        byte[] b1 = builder.points(geoPoints);
        byte[] b2 = factory.getFeatures(new MultiPoint(points)).get(0);
        assertArrayEquals(b1, b2);
    }

    public void testIssue74341() throws IOException {
        int z = 1;
        int x = 0;
        int y = 0;
        int extent = 1730;
        // this is the typical case we need to guard from.
        assertThat(hasNumericalError(z, x, y, extent), Matchers.equalTo(true));
        double lon = -171.0;
        double lat = 0.9999999403953552;
        SimpleFeatureFactory builder = new SimpleFeatureFactory(z, x, y, extent);
        FeatureFactory factory = new FeatureFactory(z, x, y, extent);
        byte[] b1 = builder.point(lon, lat);
        Point point = new Point(lon, lat);
        byte[] b2 = factory.getFeatures(point).get(0);
        assertThat(Arrays.equals(b1, b2), Matchers.equalTo(false));
    }

    private boolean hasNumericalError(int z, int x, int y, int extent) {
        final Rectangle rectangle = SphericalMercatorUtils.recToSphericalMercator(GeoTileUtils.toBoundingBox(x, y, z));
        final double xDiff = rectangle.getMaxLon() - rectangle.getMinLon();
        final double yDiff = rectangle.getMaxLat() - rectangle.getMinLat();
        return (double) -extent / yDiff != -1d / (yDiff / (double) extent) || (double) extent / xDiff != 1d / (xDiff / (double) extent);
    }

    public void testRectangle() throws IOException {
        int z = randomIntBetween(1, 10);
        int x = randomIntBetween(0, (1 << z) - 1);
        int y = randomIntBetween(0, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        SimpleFeatureFactory builder = new SimpleFeatureFactory(z, x, y, extent);
        FeatureFactory factory = new FeatureFactory(z, x, y, extent);
        Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);
        for (int i = 0; i < extent; i++) {
            byte[] b1 = builder.box(r.getMinLon(), r.getMaxLon(), r.getMinLat(), r.getMaxLat());
            byte[] b2 = factory.getFeatures(r).get(0);
            assertArrayEquals(extent + "", b1, b2);
        }
    }
}
