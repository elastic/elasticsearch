/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile.feature;

import com.wdtinc.mapbox_vector_tile.VectorTile;

import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.util.BitUtil;
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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.lessThan;

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

    public void testManyIntersectingGeometries() {
        final int z = randomIntBetween(2, 6);
        final int x = randomIntBetween(0, (1 << z) - 1);
        final int y = randomIntBetween(0, (1 << z) - 1);
        final int extent = randomIntBetween(128, 8012);
        final FeatureFactory builder = new FeatureFactory(z, x, y, extent);
        // within geometries
        assertThat(builder.getFeatures(GeoTileUtils.toBoundingBox(2 * x, 2 * y, z + 1)), iterableWithSize(1));
        assertThat(builder.getFeatures(GeoTileUtils.toBoundingBox(2 * x + 1, 2 * y, z + 1)), iterableWithSize(1));
        assertThat(builder.getFeatures(GeoTileUtils.toBoundingBox(2 * x, 2 * y + 1, z + 1)), iterableWithSize(1));
        assertThat(builder.getFeatures(GeoTileUtils.toBoundingBox(2 * x + 1, 2 * y + 1, z + 1)), iterableWithSize(1));
        // intersecting geometries
        assertThat(builder.getFeatures(expandByHalf(GeoTileUtils.toBoundingBox(2 * x, 2 * y, z + 1))), iterableWithSize(1));
        assertThat(builder.getFeatures(expandByHalf(GeoTileUtils.toBoundingBox(2 * x + 1, 2 * y, z + 1))), iterableWithSize(1));
        assertThat(builder.getFeatures(expandByHalf(GeoTileUtils.toBoundingBox(2 * x, 2 * y + 1, z + 1))), iterableWithSize(1));
        assertThat(builder.getFeatures(expandByHalf(GeoTileUtils.toBoundingBox(2 * x + 1, 2 * y + 1, z + 1))), iterableWithSize(1));
        // contain geometries
        assertThat(builder.getFeatures(GeoTileUtils.toBoundingBox(x / 4, y / 4, z - 2)), iterableWithSize(1));
        assertThat(builder.getFeatures(GeoTileUtils.toBoundingBox(x / 4, y / 4, z - 2)), iterableWithSize(1));
    }

    private Rectangle expandByHalf(Rectangle rectangle) {
        double halfWidth = (rectangle.getMaxX() - rectangle.getMinX()) / 2;
        double halfHeight = (rectangle.getMaxY() - rectangle.getMinY()) / 2;
        double minX = Math.max(-180, rectangle.getMinX() - halfWidth);
        double maxX = Math.min(180, rectangle.getMaxX() + halfWidth);
        double minY = Math.max(-GeoTileUtils.LATITUDE_MASK, rectangle.getMinY() - halfHeight);
        double maxY = Math.min(GeoTileUtils.LATITUDE_MASK, rectangle.getMaxY() + halfHeight);
        return new Rectangle(minX, maxX, maxY, minY);
    }

    public void testAntarctica() throws IOException, ParseException {
        assertParsing(new GZIPInputStream(getClass().getResourceAsStream("Antarctica.wkt.gz")));
    }

    public void testFrance() throws IOException, ParseException {
        assertParsing(new GZIPInputStream(getClass().getResourceAsStream("France.wkt.gz")));
    }

    private void assertParsing(InputStream is) throws IOException, ParseException {
        // make sure we can parse big polygons
        final BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        final Geometry geometry = WellKnownText.fromWKT(StandardValidator.instance(true), true, reader.readLine());
        final FeatureFactory builder = new FeatureFactory(0, 0, 0, 4096);
        assertThat(builder.getFeatures(geometry), iterableWithSize(1));
    }

    public void testPolygonOrientation() throws IOException {
        Polygon polygon = new Polygon(
            new LinearRing(new double[] { -10, 10, 10, -10, -10 }, new double[] { -10, -10, 10, 10, -10 }),
            org.elasticsearch.core.List.of(new LinearRing(new double[] { -5, -5, 5, 5, -5 }, new double[] { -5, 5, 5, -5, -5 }))
        );
        final FeatureFactory builder = new FeatureFactory(0, 0, 0, 4096);
        List<byte[]> bytes = builder.getFeatures(polygon);
        assertThat(bytes.size(), equalTo(1));
        final VectorTile.Tile.Feature feature = VectorTile.Tile.Feature.parseFrom(bytes.get(0));
        assertThat(feature.getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POLYGON));
        assertThat(feature.getGeometryCount(), equalTo(22));
        {
            // outer ring
            double[] xs = new double[5];
            xs[0] = BitUtil.zigZagDecode(feature.getGeometry(1));
            xs[1] = xs[0] + BitUtil.zigZagDecode(feature.getGeometry(4));
            xs[2] = xs[1] + BitUtil.zigZagDecode(feature.getGeometry(6));
            xs[3] = xs[2] + BitUtil.zigZagDecode(feature.getGeometry(8));
            xs[4] = xs[0];
            double[] ys = new double[5];
            ys[0] = BitUtil.zigZagDecode(feature.getGeometry(2));
            ys[1] = ys[0] + BitUtil.zigZagDecode(feature.getGeometry(5));
            ys[2] = ys[1] + BitUtil.zigZagDecode(feature.getGeometry(7));
            ys[3] = ys[2] + BitUtil.zigZagDecode(feature.getGeometry(9));
            ys[4] = ys[0];
            assertThat(signedArea(xs, ys), greaterThan(0.0));
        }
        {
            // inner ring
            double[] xs = new double[5];
            xs[0] = BitUtil.zigZagDecode(feature.getGeometry(12));
            xs[1] = xs[0] + BitUtil.zigZagDecode(feature.getGeometry(15));
            xs[2] = xs[1] + BitUtil.zigZagDecode(feature.getGeometry(17));
            xs[3] = xs[2] + BitUtil.zigZagDecode(feature.getGeometry(19));
            xs[4] = xs[0];
            double[] ys = new double[5];
            ys[0] = BitUtil.zigZagDecode(feature.getGeometry(13));
            ys[1] = ys[0] + BitUtil.zigZagDecode(feature.getGeometry(16));
            ys[2] = ys[1] + BitUtil.zigZagDecode(feature.getGeometry(18));
            ys[3] = ys[2] + BitUtil.zigZagDecode(feature.getGeometry(20));
            ys[4] = ys[0];
            assertThat(signedArea(xs, ys), lessThan(0.0));
        }

    }

    private double signedArea(double[] xs, double[] ys) {
        double windingSum = 0d;
        final int numPts = xs.length - 1;
        for (int i = 1, j = 0; i < numPts; j = i++) {
            // compute signed area
            windingSum += (xs[j] - xs[numPts]) * (ys[i] - ys[numPts]) - (ys[j] - ys[numPts]) * (xs[i] - xs[numPts]);
        }
        return windingSum;
    }
}
