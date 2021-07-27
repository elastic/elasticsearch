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
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;

public class FeatureFactoryTests extends ESTestCase {

    public void testPoint() {
        doTestGeometry(this::buildPoint, features -> {
            assertThat(features.size(), Matchers.equalTo(1));
            assertThat(features.get(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POINT));
        });
    }

    public void testMultiPoint() {
        doTestGeometry(this::buildMultiPoint, features -> {
            assertThat(features.size(), Matchers.equalTo(1));
            assertThat(features.get(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POINT));
        });
    }

    public void testRectangle() {
        doTestGeometry(r -> r, features -> {
            assertThat(features.size(), Matchers.equalTo(1));
            assertThat(features.get(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POLYGON));
        });
    }

    public void testLine() {
        doTestGeometry(this::buildLine, features -> {
            assertThat(features.size(), Matchers.equalTo(1));
            assertThat(features.get(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.LINESTRING));
        });
    }

    public void testMultiLine() {
        doTestGeometry(this::buildMultiLine, features -> {
            assertThat(features.size(), Matchers.equalTo(1));
            assertThat(features.get(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.LINESTRING));
        });
    }

    public void testPolygon() {
        doTestGeometry(this::buildPolygon, features -> {
            assertThat(features.size(), Matchers.equalTo(1));
            assertThat(features.get(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POLYGON));
        });
    }

    public void testMultiPolygon() {
        doTestGeometry(this::buildMultiPolygon, features -> {
            assertThat(features.size(), Matchers.equalTo(1));
            assertThat(features.get(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POLYGON));
        });
    }

    public void testGeometryCollection() {
        doTestGeometry(this::buildGeometryCollection, features -> {
            assertThat(features.size(), Matchers.equalTo(2));
            assertThat(features.get(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.LINESTRING));
            assertThat(features.get(1).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POLYGON));
        });
    }

    private void doTestGeometry(Function<Rectangle, Geometry> provider, Consumer<List<VectorTile.Tile.Feature>> consumer) {
        final int z = randomIntBetween(3, 10);
        final int x = randomIntBetween(2, (1 << z) - 1);
        final int y = randomIntBetween(2, (1 << z) - 1);
        final int extent = randomIntBetween(1 << 8, 1 << 14);
        final FeatureFactory builder = new FeatureFactory(z, x, y, extent);
        {
            final Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);
            final List<VectorTile.Tile.Feature> features = builder.getFeatures(provider.apply(r), new UserDataIgnoreConverter());
            consumer.accept(features);
        }
        {
            final Rectangle r = GeoTileUtils.toBoundingBox(x - 2, y, z);
            final List<VectorTile.Tile.Feature> features = builder.getFeatures(provider.apply(r), new UserDataIgnoreConverter());
            assertThat(features.size(), Matchers.equalTo(0));
        }
    }

    private Point buildPoint(Rectangle r) {
        final double lat = randomValueOtherThanMany((l) -> r.getMinY() >= l || r.getMaxY() <= l, GeoTestUtil::nextLatitude);
        final double lon = randomValueOtherThanMany((l) -> r.getMinX() >= l || r.getMaxX() <= l, GeoTestUtil::nextLongitude);
        return new Point(lon, lat);
    }

    private MultiPoint buildMultiPoint(Rectangle r) {
        final int numPoints = randomIntBetween(2, 10);
        final List<Point> points = new ArrayList<>(numPoints);
        for (int i = 0; i < numPoints; i++) {
            points.add(buildPoint(r));
        }
        return new MultiPoint(points);
    }

    private Line buildLine(Rectangle r) {
        return new Line(new double[] { r.getMinX(), r.getMaxX() }, new double[] { r.getMinY(), r.getMaxY() });
    }

    private MultiLine buildMultiLine(Rectangle r) {
        return new MultiLine(Collections.singletonList(buildLine(r)));
    }

    private Polygon buildPolygon(Rectangle r) {
        final LinearRing ring = new LinearRing(
            new double[] { r.getMinX(), r.getMaxX(), r.getMaxX(), r.getMinX(), r.getMinX() },
            new double[] { r.getMinY(), r.getMinY(), r.getMaxY(), r.getMaxY(), r.getMinY() }
        );
        return new Polygon(ring);
    }

    private MultiPolygon buildMultiPolygon(Rectangle r) {
        return new MultiPolygon(Collections.singletonList(buildPolygon(r)));
    }

    private GeometryCollection<Geometry> buildGeometryCollection(Rectangle r) {
        return new GeometryCollection<>(List.of(buildPolygon(r), buildLine(r)));
    }

    public void testStackOverflowError() throws Exception {
        final InputStream is = new GZIPInputStream(getClass().getResourceAsStream("polygon.wkt.gz"));
        final BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        final Geometry geometry = WellKnownText.fromWKT(StandardValidator.instance(true), true, reader.readLine());
        // just make sure we don't blow
        for (int i = 0; i < 10; i++) {
            final FeatureFactory builder = new FeatureFactory(0, 0, 0, randomIntBetween(128, 8012));
            builder.getFeatures(geometry, new UserDataIgnoreConverter());
        }
    }
}
