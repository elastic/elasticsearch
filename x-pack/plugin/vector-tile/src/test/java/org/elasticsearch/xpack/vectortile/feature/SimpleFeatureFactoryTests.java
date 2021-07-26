/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile.feature;

import org.apache.lucene.geo.GeoTestUtil;
import org.elasticsearch.geometry.Point;
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

    public void testMultiPoint() throws IOException {
        int z = randomIntBetween(3, 10);
        int x = randomIntBetween(0, (1 << z) - 1);
        int y = randomIntBetween(0, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        SimpleFeatureFactory builder = new SimpleFeatureFactory(z, x, y, extent);
        Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, z);
        int numPoints = randomIntBetween(2, 10);
        {
            List<Point> points = new ArrayList<>();
            double lat = randomValueOtherThanMany((l) -> rectangle.getMinY() >= l || rectangle.getMaxY() <= l, GeoTestUtil::nextLatitude);
            double lon = randomValueOtherThanMany((l) -> rectangle.getMinX() >= l || rectangle.getMaxX() <= l, GeoTestUtil::nextLongitude);
            points.add(new Point(lon, lat));
            for (int i = 0; i < numPoints - 1; i++) {
                points.add(new Point(GeoTestUtil.nextLongitude(), GeoTestUtil.nextLatitude()));
            }
            assertThat(builder.points(points).length, Matchers.greaterThan(0));
        }
        {
            int xNew = randomValueOtherThanMany(v -> Math.abs(v - x) < 2, () -> randomIntBetween(0, (1 << z) - 1));
            int yNew = randomValueOtherThanMany(v -> Math.abs(v - y) < 2, () -> randomIntBetween(0, (1 << z) - 1));
            Rectangle rectangleNew = GeoTileUtils.toBoundingBox(xNew, yNew, z);
            List<Point> points = new ArrayList<>();
            for (int i = 0; i < numPoints; i++) {
                double lat = randomValueOtherThanMany(
                    (l) -> rectangleNew.getMinY() >= l || rectangleNew.getMaxY() <= l,
                    GeoTestUtil::nextLatitude
                );
                double lon = randomValueOtherThanMany(
                    (l) -> rectangleNew.getMinX() >= l || rectangleNew.getMaxX() <= l,
                    GeoTestUtil::nextLongitude
                );
                points.add(new Point(lon, lat));
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
            List<Point> points = new ArrayList<>();
            double lat = randomValueOtherThanMany((l) -> rectangle.getMinY() > l || rectangle.getMaxY() < l, GeoTestUtil::nextLatitude);
            double lon = randomValueOtherThanMany((l) -> rectangle.getMinX() > l || rectangle.getMaxX() < l, GeoTestUtil::nextLongitude);
            points.add(new Point(lon, lat));
            assertArrayEquals(builder.points(points), builder.point(lon, lat));
            for (int i = 0; i < extraPoints; i++) {
                points.add(new Point(lon, lat));
            }
            assertArrayEquals(builder.points(points), builder.point(lon, lat));
        }
        {
            List<Point> points = new ArrayList<>();
            double lat = randomValueOtherThanMany((l) -> rectangle.getMinY() <= l && rectangle.getMaxY() >= l, GeoTestUtil::nextLatitude);
            double lon = randomValueOtherThanMany((l) -> rectangle.getMinX() <= l && rectangle.getMaxX() >= l, GeoTestUtil::nextLongitude);
            points.add(new Point(lon, lat));
            assertArrayEquals(builder.points(points), builder.point(lon, lat));
            for (int i = 0; i < extraPoints; i++) {
                points.add(new Point(lon, lat));
            }
            assertArrayEquals(builder.points(points), builder.point(lon, lat));
        }
    }

    public void testRectangle() throws IOException {
        int z = randomIntBetween(3, 10);
        int x = randomIntBetween(1, (1 << z) - 1);
        int y = randomIntBetween(1, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        SimpleFeatureFactory builder = new SimpleFeatureFactory(z, x, y, extent);
        {
            Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);
            assertThat(builder.box(r.getMinLon(), r.getMaxLon(), r.getMinLat(), r.getMaxLat()).length, Matchers.greaterThan(0));
        }
        {
            Rectangle r = GeoTileUtils.toBoundingBox(x - 1, y, z);
            assertThat(builder.box(r.getMinLon(), r.getMaxLon(), r.getMinLat(), r.getMaxLat()).length, Matchers.equalTo(0));
        }
    }
}
