/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile.feature;

import com.wdtinc.mapbox_vector_tile.VectorTile;

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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;

public class FeatureFactoryTests extends ESTestCase {

    public void testPoint() throws IOException {
        doTestGeometry(this::buildPoint, features -> {
            assertThat(features.size(), Matchers.equalTo(1));
            assertThat(features.get(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POINT));
        });
    }

    public void testMultiPoint() throws IOException {
        doTestGeometry(this::buildMultiPoint, features -> {
            assertThat(features.size(), Matchers.equalTo(1));
            assertThat(features.get(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POINT));
        });
    }

    public void testRectangle() throws IOException {
        doTestGeometry(r -> r, features -> {
            assertThat(features.size(), Matchers.equalTo(1));
            assertThat(features.get(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POLYGON));
        });
    }

    public void testLine() throws IOException {
        doTestGeometry(this::buildLine, features -> {
            assertThat(features.size(), Matchers.equalTo(1));
            assertThat(features.get(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.LINESTRING));
        });
    }

    public void testMultiLine() throws IOException {
        doTestGeometry(this::buildMultiLine, features -> {
            assertThat(features.size(), Matchers.equalTo(1));
            assertThat(features.get(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.LINESTRING));
        });
    }

    public void testPolygon() throws IOException {
        doTestGeometry(this::buildPolygon, features -> {
            assertThat(features.size(), Matchers.equalTo(1));
            assertThat(features.get(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POLYGON));
        });
    }

    public void testMultiPolygon() throws IOException {
        doTestGeometry(this::buildMultiPolygon, features -> {
            assertThat(features.size(), Matchers.equalTo(1));
            assertThat(features.get(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POLYGON));
        });
    }

    public void testGeometryCollection() throws IOException {
        doTestGeometry(this::buildGeometryCollection, features -> {
            assertThat(features.size(), Matchers.equalTo(2));
            assertThat(features.get(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.LINESTRING));
            assertThat(features.get(1).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POLYGON));
        });
    }

    private void doTestGeometry(Function<Rectangle, Geometry> provider, Consumer<List<VectorTile.Tile.Feature>> consumer)
        throws IOException {
        final int z = randomIntBetween(3, 10);
        final int x = randomIntBetween(2, (1 << z) - 1);
        final int y = randomIntBetween(2, (1 << z) - 1);
        final int extent = randomIntBetween(1 << 8, 1 << 14);
        final FeatureFactory builder = new FeatureFactory(z, x, y, extent);
        {
            final Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);
            final List<byte[]> byteFeatures = builder.getFeatures(provider.apply(r));
            final List<VectorTile.Tile.Feature> features = new ArrayList<>(byteFeatures.size());
            for (byte[] byteFeature : byteFeatures) {
                features.add(VectorTile.Tile.Feature.parseFrom(byteFeature));
            }
            consumer.accept(features);
        }
        {
            final Rectangle r = GeoTileUtils.toBoundingBox(x - 2, y, z);
            final List<byte[]> byteFeatures = builder.getFeatures(provider.apply(r));
            assertThat(byteFeatures.size(), Matchers.equalTo(0));
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
        return new GeometryCollection<>(org.elasticsearch.core.List.of(buildPolygon(r), buildLine(r)));
    }

    public void testStackOverflowError() throws IOException, ParseException {
        // The provided polygon contains 49K points and we have observed that for some tiles and some extent values,
        // it makes the library we are using to compute features to fail with a StackOverFlowError. This test just makes
        // sure the fix in place avoids that error.
        final InputStream is = new GZIPInputStream(getClass().getResourceAsStream("polygon.wkt.gz"));
        final BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        final Geometry geometry = WellKnownText.fromWKT(StandardValidator.instance(true), true, reader.readLine());
        for (int i = 0; i < 10; i++) {
            final int z = randomIntBetween(0, 4);
            final int x = randomIntBetween(0, (1 << z) - 1);
            final int y = randomIntBetween(0, (1 << z) - 1);
            final int extent = randomIntBetween(128, 8012);
            final FeatureFactory builder = new FeatureFactory(z, x, y, extent);
            try {
                builder.getFeatures(geometry);
            } catch (StackOverflowError error) {
                fail("stackoverflow error thrown at " + z + "/" + x + "/" + y + "@" + extent);
            }
        }
    }

    public void testTileInsidePolygon() throws Exception {
        final int z = randomIntBetween(0, 4);
        final int x = randomIntBetween(0, (1 << z) - 1);
        final int y = randomIntBetween(0, (1 << z) - 1);
        final int extent = randomIntBetween(128, 8012);
        final FeatureFactory builder = new FeatureFactory(z, x, y, extent);
        final Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, z);
        final double minX = Math.max(-180, rectangle.getMinX() - 1);
        final double maxX = Math.min(180, rectangle.getMaxX() + 1);
        final double minY = Math.max(-GeoTileUtils.LATITUDE_MASK, rectangle.getMinY() - 1);
        final double maxY = Math.min(GeoTileUtils.LATITUDE_MASK, rectangle.getMaxY() + 1);
        Polygon bigPolygon = new Polygon(
            new LinearRing(new double[] { minX, maxX, maxX, minX, minX }, new double[] { minY, minY, maxY, maxY, minY })
        );
        final List<byte[]> bytes = builder.getFeatures(bigPolygon);
        assertThat(bytes, Matchers.iterableWithSize(1));
        final VectorTile.Tile.Feature feature = VectorTile.Tile.Feature.parseFrom(bytes.get(0));
        assertThat(feature.getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POLYGON));
    }
}
