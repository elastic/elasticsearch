/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile.feature;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.adapt.jts.UserDataIgnoreConverter;

import org.apache.lucene.geo.GeoTestUtil;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FeatureFactoryTests extends ESTestCase {

    public void testPoint() {
        int z = randomIntBetween(3, 10);
        int x = randomIntBetween(0, (1 << z) - 1);
        int y = randomIntBetween(0, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        FeatureFactory builder = new FeatureFactory(z, x, y, extent);
        Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, z);
        {
            double lat = randomValueOtherThanMany((l) -> rectangle.getMinY() >= l || rectangle.getMaxY() <= l, GeoTestUtil::nextLatitude);
            double lon = randomValueOtherThanMany((l) -> rectangle.getMinX() >= l || rectangle.getMaxX() <= l, GeoTestUtil::nextLongitude);
            List<VectorTile.Tile.Feature> features = builder.getFeatures(new Point(lon, lat), new UserDataIgnoreConverter());
            assertThat(features.size(), Matchers.equalTo(1));
            VectorTile.Tile.Feature feature = features.get(0);
            assertThat(feature.getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POINT));
        }
        {
            int xNew = randomValueOtherThanMany(v -> Math.abs(v - x) < 2, () -> randomIntBetween(0, (1 << z) - 1));
            int yNew = randomValueOtherThanMany(v -> Math.abs(v - y) < 2, () -> randomIntBetween(0, (1 << z) - 1));
            Rectangle rectangleNew = GeoTileUtils.toBoundingBox(xNew, yNew, z);
            double lat = randomValueOtherThanMany(
                (l) -> rectangleNew.getMinY() >= l || rectangleNew.getMaxY() <= l,
                GeoTestUtil::nextLatitude
            );
            double lon = randomValueOtherThanMany(
                (l) -> rectangleNew.getMinX() >= l || rectangleNew.getMaxX() <= l,
                GeoTestUtil::nextLongitude
            );
            List<VectorTile.Tile.Feature> features = builder.getFeatures(new Point(lon, lat), new UserDataIgnoreConverter());
            assertThat(features.size(), Matchers.equalTo(0));
        }
    }

    public void testMultiPoint() {
        int z = randomIntBetween(3, 10);
        int x = randomIntBetween(0, (1 << z) - 1);
        int y = randomIntBetween(0, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        FeatureFactory builder = new FeatureFactory(z, x, y, extent);
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
            List<VectorTile.Tile.Feature> features = builder.getFeatures(new MultiPoint(points), new UserDataIgnoreConverter());
            assertThat(features.size(), Matchers.equalTo(1));
            VectorTile.Tile.Feature feature = features.get(0);
            assertThat(feature.getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POINT));
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
            List<VectorTile.Tile.Feature> features = builder.getFeatures(new MultiPoint(points), new UserDataIgnoreConverter());
            assertThat(features.size(), Matchers.equalTo(0));
        }
    }

    public void testRectangle() {
        int z = randomIntBetween(3, 10);
        int x = randomIntBetween(2, (1 << z) - 1);
        int y = randomIntBetween(2, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        FeatureFactory builder = new FeatureFactory(z, x, y, extent);
        {
            Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);
            List<VectorTile.Tile.Feature> features = builder.getFeatures(r, new UserDataIgnoreConverter());
            assertThat(features.size(), Matchers.equalTo(1));
            VectorTile.Tile.Feature feature = features.get(0);
            assertThat(feature.getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POLYGON));
        }
        {
            Rectangle r = GeoTileUtils.toBoundingBox(x - 2, y, z);
            List<VectorTile.Tile.Feature> features = builder.getFeatures(r, new UserDataIgnoreConverter());
            assertThat(features.size(), Matchers.equalTo(0));
        }
    }

    public void testLine() {
        int z = randomIntBetween(3, 10);
        int x = randomIntBetween(2, (1 << z) - 1);
        int y = randomIntBetween(2, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        FeatureFactory builder = new FeatureFactory(z, x, y, extent);
        {
            Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);
            List<VectorTile.Tile.Feature> features = builder.getFeatures(buildLine(r), new UserDataIgnoreConverter());
            assertThat(features.size(), Matchers.equalTo(1));
            VectorTile.Tile.Feature feature = features.get(0);
            assertThat(feature.getType(), Matchers.equalTo(VectorTile.Tile.GeomType.LINESTRING));
        }
        {
            Rectangle r = GeoTileUtils.toBoundingBox(x - 2, y, z);
            List<VectorTile.Tile.Feature> features = builder.getFeatures(buildLine(r), new UserDataIgnoreConverter());
            assertThat(features.size(), Matchers.equalTo(0));
        }
    }

    public void testMultiLine() {
        int z = randomIntBetween(3, 10);
        int x = randomIntBetween(2, (1 << z) - 1);
        int y = randomIntBetween(2, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        FeatureFactory builder = new FeatureFactory(z, x, y, extent);
        {
            Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);
            List<VectorTile.Tile.Feature> features = builder.getFeatures(buildMultiLine(r), new UserDataIgnoreConverter());
            assertThat(features.size(), Matchers.equalTo(1));
            VectorTile.Tile.Feature feature = features.get(0);
            assertThat(feature.getType(), Matchers.equalTo(VectorTile.Tile.GeomType.LINESTRING));
        }
        {
            Rectangle r = GeoTileUtils.toBoundingBox(x - 2, y, z);
            List<VectorTile.Tile.Feature> features = builder.getFeatures(buildMultiLine(r), new UserDataIgnoreConverter());
            assertThat(features.size(), Matchers.equalTo(0));
        }
    }

    public void testPolygon() {
        int z = randomIntBetween(3, 10);
        int x = randomIntBetween(2, (1 << z) - 1);
        int y = randomIntBetween(2, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        FeatureFactory builder = new FeatureFactory(z, x, y, extent);
        {
            Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);
            List<VectorTile.Tile.Feature> features = builder.getFeatures(buildPolygon(r), new UserDataIgnoreConverter());
            assertThat(features.size(), Matchers.equalTo(1));
            VectorTile.Tile.Feature feature = features.get(0);
            assertThat(feature.getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POLYGON));
        }
        {
            Rectangle r = GeoTileUtils.toBoundingBox(x - 2, y, z);
            List<VectorTile.Tile.Feature> features = builder.getFeatures(buildPolygon(r), new UserDataIgnoreConverter());
            assertThat(features.size(), Matchers.equalTo(0));
        }
    }

    public void testMultiPolygon() {
        int z = randomIntBetween(3, 10);
        int x = randomIntBetween(2, (1 << z) - 1);
        int y = randomIntBetween(2, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        FeatureFactory builder = new FeatureFactory(z, x, y, extent);
        {
            Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);
            List<VectorTile.Tile.Feature> features = builder.getFeatures(buildMultiPolygon(r), new UserDataIgnoreConverter());
            assertThat(features.size(), Matchers.equalTo(1));
            VectorTile.Tile.Feature feature = features.get(0);
            assertThat(feature.getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POLYGON));
        }
        {
            Rectangle r = GeoTileUtils.toBoundingBox(x - 2, y, z);
            List<VectorTile.Tile.Feature> features = builder.getFeatures(buildMultiPolygon(r), new UserDataIgnoreConverter());
            assertThat(features.size(), Matchers.equalTo(0));
        }
    }

    public void testGeometryCollection() {
        int z = randomIntBetween(3, 10);
        int x = randomIntBetween(2, (1 << z) - 1);
        int y = randomIntBetween(2, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        FeatureFactory builder = new FeatureFactory(z, x, y, extent);
        {
            Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);
            List<Geometry> geometries = List.of(buildPolygon(r), buildLine(r));
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> builder.getFeatures(new GeometryCollection<>(geometries), new UserDataIgnoreConverter())
            );
            assertThat(ex.getMessage(), Matchers.equalTo("GeometryCollection is not supported"));
        }
    }

    private Line buildLine(Rectangle r) {
        return new Line(new double[] { r.getMinX(), r.getMaxX() }, new double[] { r.getMinY(), r.getMaxY() });
    }

    private MultiLine buildMultiLine(Rectangle r) {
        return new MultiLine(Collections.singletonList(buildLine(r)));
    }

    private Polygon buildPolygon(Rectangle r) {
        LinearRing ring = new LinearRing(
            new double[] { r.getMinX(), r.getMaxX(), r.getMaxX(), r.getMinX(), r.getMinX() },
            new double[] { r.getMinY(), r.getMinY(), r.getMaxY(), r.getMaxY(), r.getMinY() }
        );
        return new Polygon(ring);
    }

    private MultiPolygon buildMultiPolygon(Rectangle r) {
        return new MultiPolygon(Collections.singletonList(buildPolygon(r)));
    }
}
