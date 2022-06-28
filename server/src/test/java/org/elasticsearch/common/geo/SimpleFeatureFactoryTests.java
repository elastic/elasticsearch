/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.apache.lucene.geo.GeoTestUtil;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimpleFeatureFactoryTests extends ESTestCase {

    public void testPoint() throws IOException {
        int z = randomIntBetween(3, 10);
        int x = randomIntBetween(0, (1 << z) - 1);
        int y = randomIntBetween(0, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        SimpleFeatureFactory builder = new SimpleFeatureFactory(z, x, y, extent);
        Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, z);
        {
            double lat = randomValueOtherThanMany(l -> rectangle.getMinY() >= l || rectangle.getMaxY() <= l, GeoTestUtil::nextLatitude);
            double lon = randomValueOtherThanMany(l -> rectangle.getMinX() >= l || rectangle.getMaxX() <= l, GeoTestUtil::nextLongitude);
            assertThat(builder.point(lon, lat).length, Matchers.greaterThan(0));
        }
        {
            int xNew = randomValueOtherThanMany(v -> Math.abs(v - x) < 2, () -> randomIntBetween(0, (1 << z) - 1));
            int yNew = randomValueOtherThanMany(v -> Math.abs(v - y) < 2, () -> randomIntBetween(0, (1 << z) - 1));
            Rectangle rectangleNew = GeoTileUtils.toBoundingBox(xNew, yNew, z);
            double lat = randomValueOtherThanMany(
                l -> rectangleNew.getMinY() >= l || rectangleNew.getMaxY() <= l,
                GeoTestUtil::nextLatitude
            );
            double lon = randomValueOtherThanMany(
                (l) -> rectangleNew.getMinX() >= l || rectangleNew.getMaxX() <= l,
                GeoTestUtil::nextLongitude
            );
            assertThat(builder.point(lon, lat).length, Matchers.equalTo(0));
        }
    }

    public void testMultiPoint() {
        int z = randomIntBetween(3, 10);
        int x = randomIntBetween(0, (1 << z) - 1);
        int y = randomIntBetween(0, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        SimpleFeatureFactory builder = new SimpleFeatureFactory(z, x, y, extent);
        Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, z);
        int numPoints = randomIntBetween(2, 10);
        {
            List<GeoPoint> points = new ArrayList<>();
            double lat = randomValueOtherThanMany((l) -> rectangle.getMinY() >= l || rectangle.getMaxY() <= l, GeoTestUtil::nextLatitude);
            double lon = randomValueOtherThanMany((l) -> rectangle.getMinX() >= l || rectangle.getMaxX() <= l, GeoTestUtil::nextLongitude);
            points.add(new GeoPoint(lat, lon));
            for (int i = 0; i < numPoints - 1; i++) {
                points.add(new GeoPoint(GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude()));
            }
            assertThat(builder.points(points).length, Matchers.greaterThan(0));
        }
        {
            int xNew = randomValueOtherThanMany(v -> Math.abs(v - x) < 2, () -> randomIntBetween(0, (1 << z) - 1));
            int yNew = randomValueOtherThanMany(v -> Math.abs(v - y) < 2, () -> randomIntBetween(0, (1 << z) - 1));
            Rectangle rectangleNew = GeoTileUtils.toBoundingBox(xNew, yNew, z);
            List<GeoPoint> points = new ArrayList<>();
            for (int i = 0; i < numPoints; i++) {
                double lat = randomValueOtherThanMany(
                    (l) -> rectangleNew.getMinY() >= l || rectangleNew.getMaxY() <= l,
                    GeoTestUtil::nextLatitude
                );
                double lon = randomValueOtherThanMany(
                    (l) -> rectangleNew.getMinX() >= l || rectangleNew.getMaxX() <= l,
                    GeoTestUtil::nextLongitude
                );
                points.add(new GeoPoint(lat, lon));
            }
            assertThat(builder.points(points).length, Matchers.equalTo(0));
        }
    }

    public void testPointsMethodConsistency() throws IOException {
        int z = randomIntBetween(1, 10);
        int x = randomIntBetween(0, (1 << z) - 1);
        int y = randomIntBetween(0, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        SimpleFeatureFactory builder = new SimpleFeatureFactory(z, x, y, extent);
        Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, z);
        int extraPoints = randomIntBetween(1, 10);
        {
            List<GeoPoint> points = new ArrayList<>();
            double lat = randomValueOtherThanMany((l) -> rectangle.getMinY() > l || rectangle.getMaxY() < l, GeoTestUtil::nextLatitude);
            double lon = randomValueOtherThanMany((l) -> rectangle.getMinX() > l || rectangle.getMaxX() < l, GeoTestUtil::nextLongitude);
            points.add(new GeoPoint(lat, lon));
            assertArrayEquals(builder.points(points), builder.point(lon, lat));
            for (int i = 0; i < extraPoints; i++) {
                points.add(new GeoPoint(lat, lon));
            }
            assertArrayEquals(builder.points(points), builder.point(lon, lat));
        }
        {
            List<GeoPoint> points = new ArrayList<>();
            double lat = randomValueOtherThanMany((l) -> rectangle.getMinY() <= l && rectangle.getMaxY() >= l, GeoTestUtil::nextLatitude);
            double lon = randomValueOtherThanMany((l) -> rectangle.getMinX() <= l && rectangle.getMaxX() >= l, GeoTestUtil::nextLongitude);
            points.add(new GeoPoint(lat, lon));
            assertArrayEquals(builder.points(points), builder.point(lon, lat));
            for (int i = 0; i < extraPoints; i++) {
                points.add(new GeoPoint(lat, lon));
            }
            assertArrayEquals(builder.points(points), builder.point(lon, lat));
        }
    }

    public void testRectangle() throws IOException {
        int z = randomIntBetween(3, 10);
        int x = randomIntBetween(2, (1 << z) - 1);
        int y = randomIntBetween(2, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        SimpleFeatureFactory builder = new SimpleFeatureFactory(z, x, y, extent);
        {
            Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);
            assertThat(builder.box(r.getMinLon(), r.getMaxLon(), r.getMinLat(), r.getMaxLat()).length, Matchers.greaterThan(0));
        }
        {
            Rectangle r = GeoTileUtils.toBoundingBox(x - 2, y, z);
            assertThat(builder.box(r.getMinLon(), r.getMaxLon(), r.getMinLat(), r.getMaxLat()).length, Matchers.equalTo(0));
        }
    }

    public void testDegeneratedRectangle() throws IOException {
        int z = randomIntBetween(3, 10);
        int x = randomIntBetween(1, (1 << z) - 1);
        int y = randomIntBetween(1, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        SimpleFeatureFactory builder = new SimpleFeatureFactory(z, x, y, extent);
        {
            Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);
            // box is a point
            assertThat(builder.box(r.getMaxLon(), r.getMaxLon(), r.getMaxLat(), r.getMaxLat()).length, Matchers.greaterThan(0));
            assertThat(builder.box(r.getMaxLon(), r.getMaxLon(), r.getMinLat(), r.getMinLat()).length, Matchers.greaterThan(0));
            assertThat(builder.box(r.getMinLon(), r.getMinLon(), r.getMaxLat(), r.getMaxLat()).length, Matchers.greaterThan(0));
            assertThat(builder.box(r.getMinLon(), r.getMinLon(), r.getMinLat(), r.getMinLat()).length, Matchers.greaterThan(0));
        }
        {
            Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);
            // box is a line
            assertThat(builder.box(r.getMinLon(), r.getMinLon(), r.getMinLat(), r.getMaxLat()).length, Matchers.greaterThan(0));
            assertThat(builder.box(r.getMinLon(), r.getMaxLon(), r.getMinLat(), r.getMinLat()).length, Matchers.greaterThan(0));
        }
    }
}
