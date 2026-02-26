/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.DimensionalShapeType;
import org.elasticsearch.lucene.spatial.Extent;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.elasticsearch.geo.GeometryTestUtils.randomLine;
import static org.elasticsearch.geo.GeometryTestUtils.randomMultiLine;
import static org.elasticsearch.geo.GeometryTestUtils.randomMultiPoint;
import static org.elasticsearch.geo.GeometryTestUtils.randomMultiPolygon;
import static org.elasticsearch.geo.GeometryTestUtils.randomPoint;
import static org.elasticsearch.geo.GeometryTestUtils.randomPolygon;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.oneOf;

public class GeometryDocValueTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testDimensionalShapeType() throws IOException {
        assertDimensionalShapeType(randomPoint(false), DimensionalShapeType.POINT);
        assertDimensionalShapeType(randomMultiPoint(false), DimensionalShapeType.POINT);
        assertDimensionalShapeType(randomLine(false), DimensionalShapeType.LINE);
        assertDimensionalShapeType(randomMultiLine(false), DimensionalShapeType.LINE);
        Geometry randoPoly = randomValueOtherThanMany(g -> {
            try {
                Geometry newGeo = GeometryNormalizer.apply(Orientation.CCW, g);
                return newGeo.type() != ShapeType.POLYGON;
            } catch (Exception e) {
                return true;
            }
        }, () -> randomPolygon(false));
        Geometry randoMultiPoly = randomValueOtherThanMany(g -> {
            try {
                Geometry newGeo = GeometryNormalizer.apply(Orientation.CCW, g);
                return newGeo.type() != ShapeType.MULTIPOLYGON;
            } catch (Exception e) {
                return true;
            }
        }, () -> randomMultiPolygon(false));
        assertDimensionalShapeType(randoPoly, DimensionalShapeType.POLYGON);
        assertDimensionalShapeType(randoMultiPoly, DimensionalShapeType.POLYGON);
        assertDimensionalShapeType(
            randomFrom(
                new GeometryCollection<>(List.of(randomPoint(false))),
                new GeometryCollection<>(List.of(randomMultiPoint(false))),
                new GeometryCollection<>(
                    Collections.singletonList(new GeometryCollection<>(List.of(randomPoint(false), randomMultiPoint(false))))
                )
            ),
            DimensionalShapeType.POINT
        );
        assertDimensionalShapeType(
            randomFrom(
                new GeometryCollection<>(List.of(randomPoint(false), randomLine(false))),
                new GeometryCollection<>(List.of(randomMultiPoint(false), randomMultiLine(false))),
                new GeometryCollection<>(
                    Collections.singletonList(new GeometryCollection<>(List.of(randomPoint(false), randomLine(false))))
                )
            ),
            DimensionalShapeType.LINE
        );
        assertDimensionalShapeType(
            randomFrom(
                new GeometryCollection<>(
                    List.of(randomPoint(false), GeometryNormalizer.apply(Orientation.CCW, randomLine(false)), randoPoly)
                ),
                new GeometryCollection<>(List.of(randomMultiPoint(false), randoMultiPoly)),
                new GeometryCollection<>(
                    Collections.singletonList(
                        new GeometryCollection<>(
                            List.of(
                                GeometryNormalizer.apply(Orientation.CCW, randomLine(false)),
                                GeometryNormalizer.apply(Orientation.CCW, randoPoly)
                            )
                        )
                    )
                )
            ),
            DimensionalShapeType.POLYGON
        );
    }

    public void testRectangleShape() throws IOException {
        for (int i = 0; i < 1000; i++) {
            int minX = randomIntBetween(-40, -1);
            int maxX = randomIntBetween(1, 40);
            int minY = randomIntBetween(-40, -1);
            int maxY = randomIntBetween(1, 40);
            Rectangle rectangle = new Rectangle(minX, maxX, maxY, minY);
            GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(rectangle, CoordinateEncoder.GEO);

            Extent expectedExtent = getExtentFromBox(minX, minY, maxX, maxY);
            assertThat("Rectangle extent", reader.getExtent(), equalTo(expectedExtent));
            // centroid is calculated using original double values but then loses precision as it is serialized as an integer
            int encodedCentroidX = CoordinateEncoder.GEO.encodeX(((double) minX + maxX) / 2);
            int encodedCentroidY = CoordinateEncoder.GEO.encodeY(((double) minY + maxY) / 2);
            assertEquals(encodedCentroidX, reader.getCentroidX());
            assertEquals(encodedCentroidY, reader.getCentroidY());

            // Label position is the centroid if within the polygon
            GeoShapeValues.GeoShapeValue shapeValue = GeoTestUtils.geoShapeValue(rectangle);
            SpatialPoint labelPosition = shapeValue.labelPosition();
            double labelX = ((double) minX + maxX) / 2;
            double labelY = ((double) minY + maxY) / 2;
            assertEquals(labelX, labelPosition.getX(), 0.0000001);
            assertEquals(labelY, labelPosition.getY(), 0.0000001);

            // Assert that we can extract the original geometry from the doc-values
            assertThat("Expect equivalent geometry", reader.getGeometry(CoordinateEncoder.GEO), equivalentTo(rectangle));
        }
    }

    public void testNonCentroidPolygon() throws IOException {
        final Rectangle r1 = new Rectangle(-10, -5, 10, -10);
        final Rectangle r2 = new Rectangle(5, 10, 10, -10);
        MultiPolygon geometry = new MultiPolygon(List.of(toPolygon(r1), toPolygon(r2)));
        GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(geometry, CoordinateEncoder.GEO);

        // Centroid is at the origin
        int encodedCentroidX = CoordinateEncoder.GEO.encodeX(0);
        int encodedCentroidY = CoordinateEncoder.GEO.encodeY(0);
        assertEquals(encodedCentroidX, reader.getCentroidX());
        assertEquals(encodedCentroidY, reader.getCentroidY());

        // Label position is calculated as the first triangle
        GeoShapeValues.GeoShapeValue shapeValue = GeoTestUtils.geoShapeValue(geometry);
        SpatialPoint labelPosition = shapeValue.labelPosition();
        assertThat(
            "Expect label position to match one of eight triangles in the two rectangles",
            new GeoPoint(labelPosition),
            isRectangleLabelPosition(r1, r2)
        );

        Geometry extracted = reader.getGeometry(CoordinateEncoder.GEO);
        assertThat("Expect equivalent geometry", extracted, equivalentTo(geometry));
    }

    public void testAntarcticaLabelPosition() throws Exception {
        Geometry geometry = loadResourceAsGeometry("Antarctica.wkt.gz");
        GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(geometry, CoordinateEncoder.GEO);

        // Centroid is near the South Pole
        int encodedLatThreshold = CoordinateEncoder.GEO.encodeY(-80);
        assertThat(
            "Centroid should be near the South Pole, or further South than -80 degrees",
            reader.getCentroidY(),
            lessThan(encodedLatThreshold)
        );

        // Label position is the centroid if within the polygon
        GeoShapeValues.GeoShapeValue shapeValue = GeoTestUtils.geoShapeValue(geometry);
        SpatialPoint labelPosition = shapeValue.labelPosition();
        double centroidX = CoordinateEncoder.GEO.decodeX(reader.getCentroidX());
        double centroidY = CoordinateEncoder.GEO.decodeY(reader.getCentroidY());
        assertEquals(centroidX, labelPosition.getX(), 0.0000001);
        assertEquals(centroidY, labelPosition.getY(), 0.0000001);
        org.apache.lucene.geo.Circle tolerance = new org.apache.lucene.geo.Circle(centroidY, centroidX, 1);
        assertTrue("Expect label position to be within the geometry", shapeValue.relate(tolerance) != GeoRelation.QUERY_DISJOINT);

        // Assert that we can extract the original geometry from the doc-values
        assertThat("Expect equivalent geometry", reader.getGeometry(CoordinateEncoder.GEO), equivalentTo(geometry));
    }

    public void testFranceLabelPosition() throws Exception {
        Geometry geometry = loadResourceAsGeometry("France.wkt.gz");
        GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(geometry, CoordinateEncoder.GEO);

        // Label position is the centroid if within the polygon
        GeoShapeValues.GeoShapeValue shapeValue = GeoTestUtils.geoShapeValue(geometry);
        SpatialPoint labelPosition = shapeValue.labelPosition();
        double centroidX = CoordinateEncoder.GEO.decodeX(reader.getCentroidX());
        double centroidY = CoordinateEncoder.GEO.decodeY(reader.getCentroidY());
        assertEquals(centroidX, labelPosition.getX(), 0.0000001);
        assertEquals(centroidY, labelPosition.getY(), 0.0000001);
        org.apache.lucene.geo.Circle tolerance = new org.apache.lucene.geo.Circle(centroidY, centroidX, 1);
        assertTrue("Expect label position to be within the geometry", shapeValue.relate(tolerance) != GeoRelation.QUERY_DISJOINT);

        // Assert that we can extract the original geometry from the doc-values
        assertThat("Expect equivalent geometry", reader.getGeometry(CoordinateEncoder.GEO), equivalentTo(geometry));
    }

    public void testRandomGeometryReconstruction() throws IOException {
        for (int i = 0; i < 100; i++) {
            Geometry geometry = switch (i % 7) {
                case 0 -> randomPoint(false);
                case 1 -> randomMultiPoint(false);
                case 2 -> randomLine(false);
                case 3 -> randomMultiLine(false);
                case 4 -> randomPolygon(false);
                case 5 -> randomMultiPolygon(false);
                case 6 -> new Rectangle(
                    randomIntBetween(-179, -1),
                    randomIntBetween(1, 179),
                    randomIntBetween(1, 89),
                    randomIntBetween(-89, -1)
                );
                default -> throw new AssertionError();
            };
            GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(geometry, CoordinateEncoder.GEO);
            Geometry extracted = reader.getGeometry(CoordinateEncoder.GEO);
            assertNotNull("Iteration " + i + ": expected non-null geometry for " + geometry.type(), extracted);
            assertThat("Iteration " + i + ": equivalent geometry for " + geometry.type(), extracted, equivalentTo(geometry));
        }
    }

    private Geometry loadResourceAsGeometry(String filename) throws IOException, ParseException {
        GZIPInputStream is = new GZIPInputStream(getClass().getResourceAsStream(filename));
        final BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        final Geometry geometry = WellKnownText.fromWKT(StandardValidator.instance(true), true, reader.readLine());
        return geometry;
    }

    private Polygon toPolygon(Rectangle r) {
        return new Polygon(
            new LinearRing(
                new double[] { r.getMinX(), r.getMaxX(), r.getMaxX(), r.getMinX(), r.getMinX() },
                new double[] { r.getMinY(), r.getMinY(), r.getMaxY(), r.getMaxY(), r.getMinY() }
            )
        );
    }

    private static RectangleLabelPosition isRectangleLabelPosition(Rectangle... rectangles) {
        return new RectangleLabelPosition(rectangles);
    }

    private static class RectangleLabelPosition extends TypeSafeMatcher<GeoPoint> {
        private final Point[] encodedPositions;

        private RectangleLabelPosition(Rectangle... rectangles) {
            encodedPositions = new Point[rectangles.length * 4];
            for (int i = 0; i < rectangles.length; i++) {
                Rectangle rectangle = rectangles[i];
                GeoPoint a = new GeoPoint(rectangle.getMinY(), rectangle.getMinX());
                GeoPoint b = new GeoPoint(rectangle.getMinY(), rectangle.getMaxX());
                GeoPoint c = new GeoPoint(rectangle.getMaxY(), rectangle.getMaxX());
                GeoPoint d = new GeoPoint(rectangle.getMaxY(), rectangle.getMinX());
                encodedPositions[i * 4 + 0] = average(a, b, c);
                encodedPositions[i * 4 + 1] = average(b, c, d);
                encodedPositions[i * 4 + 2] = average(c, d, a);
                encodedPositions[i * 4 + 3] = average(d, a, b);
            }
        }

        private Point average(GeoPoint... points) {
            double lon = 0;
            double lat = 0;
            for (GeoPoint point : points) {
                lon += point.lon();
                lat += point.lat();
            }
            int x = CoordinateEncoder.GEO.encodeX(lon / points.length);
            int y = CoordinateEncoder.GEO.encodeY(lat / points.length);
            return new Point(x, y);
        }

        @Override
        public boolean matchesSafely(GeoPoint point) {
            int x = CoordinateEncoder.GEO.encodeX(point.lon());
            int y = CoordinateEncoder.GEO.encodeY(point.lat());
            return is(oneOf(encodedPositions)).matches(new Point(x, y));
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("is(oneOf(" + Arrays.toString(encodedPositions) + ")");
        }
    }

    private static Extent getExtentFromBox(double bottomLeftX, double bottomLeftY, double topRightX, double topRightY) {
        return Extent.fromPoints(
            CoordinateEncoder.GEO.encodeX(bottomLeftX),
            CoordinateEncoder.GEO.encodeY(bottomLeftY),
            CoordinateEncoder.GEO.encodeX(topRightX),
            CoordinateEncoder.GEO.encodeY(topRightY)
        );

    }

    private static void assertDimensionalShapeType(Geometry geometry, DimensionalShapeType expected) throws IOException {
        GeometryDocValueReader reader = GeoTestUtils.geometryDocValueReader(geometry, CoordinateEncoder.GEO);
        assertThat(reader.getDimensionalShapeType(), equalTo(expected));
    }

    private Matcher<? super Geometry> equivalentTo(Geometry geometry) {
        return new TestGeometryEquivalence(geometry, CoordinateEncoder.GEO);
    }

    /**
     * Hamcrest matcher that checks two geometries are structurally identical (same topology)
     * with coordinates equivalent within the Lucene quantization tolerance. Coordinates are
     * compared by encoding both sides to the integer grid and checking equality, so any
     * difference smaller than one quantization step is accepted.
     */
    private static class TestGeometryEquivalence extends BaseMatcher<Geometry> {

        private final Geometry expected;
        private final CoordinateEncoder encoder;
        private String mismatch;

        private TestGeometryEquivalence(Geometry expected, CoordinateEncoder encoder) {
            this.expected = expected;
            this.encoder = encoder;
        }

        @Override
        public boolean matches(Object o) {
            if (o instanceof Geometry actual) {
                mismatch = checkEquivalent(expected, actual, "");
                return mismatch == null;
            }
            mismatch = "not a Geometry: " + (o == null ? "null" : o.getClass().getName());
            return false;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("geometry equivalent (within quantization) to ").appendValue(WellKnownText.toWKT(expected));
        }

        @Override
        public void describeMismatch(Object item, Description description) {
            if (mismatch != null) {
                description.appendText(mismatch);
            }
        }

        private String checkEquivalent(Geometry expected, Geometry actual, String path) {
            return expected.visit(new EquivalenceVisitor(actual, path));
        }

        private class EquivalenceVisitor implements GeometryVisitor<String, RuntimeException> {
            private final Geometry actual;
            private final String path;

            EquivalenceVisitor(Geometry actual, String path) {
                this.actual = actual;
                this.path = path;
            }

            @Override
            public String visit(Circle circle) {
                return path + ": Circle comparison not supported";
            }

            @Override
            public String visit(GeometryCollection<?> expected) {
                if (actual instanceof GeometryCollection<?> a) {
                    if (expected.size() != a.size()) return sizeMismatch(expected.size(), a.size());
                    for (int i = 0; i < expected.size(); i++) {
                        String r = checkEquivalent(expected.get(i), a.get(i), path + "[" + i + "]");
                        if (r != null) return r;
                    }
                    return null;
                }
                return typeMismatch(expected);
            }

            @Override
            public String visit(Line expected) {
                if (actual instanceof Line a) {
                    return compareLineCoords(path, expected, a);
                }
                return typeMismatch(expected);
            }

            @Override
            public String visit(LinearRing expected) {
                if (actual instanceof LinearRing a) {
                    return compareLineCoords(path, expected, a);
                }
                return typeMismatch(expected);
            }

            @Override
            public String visit(MultiLine expected) {
                if (actual instanceof MultiLine a) {
                    if (expected.size() != a.size()) return sizeMismatch(expected.size(), a.size());
                    for (int i = 0; i < expected.size(); i++) {
                        String r = checkEquivalent(expected.get(i), a.get(i), path + "[" + i + "]");
                        if (r != null) return r;
                    }
                    return null;
                }
                return typeMismatch(expected);
            }

            @Override
            public String visit(MultiPoint expected) {
                if (actual instanceof MultiPoint a) {
                    if (expected.size() != a.size()) return sizeMismatch(expected.size(), a.size());
                    // Point ordering in MultiPoint is semantically meaningless and is not
                    // preserved by the triangle tree (legacy format), so match greedily.
                    boolean[] used = new boolean[a.size()];
                    for (int i = 0; i < expected.size(); i++) {
                        Point ep = expected.get(i);
                        boolean found = false;
                        for (int j = 0; j < a.size(); j++) {
                            if (used[j] == false) {
                                Point ap = a.get(j);
                                if (compareCoord("", ep.getX(), ep.getY(), ap.getX(), ap.getY()) == null) {
                                    used[j] = true;
                                    found = true;
                                    break;
                                }
                            }
                        }
                        if (found == false) {
                            return path + ": no matching point for expected[" + i + "] (" + ep.getX() + ", " + ep.getY() + ") in " + a;
                        }
                    }
                    return null;
                }
                return typeMismatch(expected);
            }

            @Override
            public String visit(MultiPolygon expected) {
                if (actual instanceof MultiPolygon a) {
                    if (expected.size() != a.size()) return sizeMismatch(expected.size(), a.size());
                    for (int i = 0; i < expected.size(); i++) {
                        String r = checkEquivalent(expected.get(i), a.get(i), path + "[" + i + "]");
                        if (r != null) return r;
                    }
                    return null;
                }
                return typeMismatch(expected);
            }

            @Override
            public String visit(Point expected) {
                if (actual instanceof Point a) {
                    return compareCoord(path, expected.getX(), expected.getY(), a.getX(), a.getY());
                }
                return typeMismatch(expected);
            }

            @Override
            public String visit(Polygon expected) {
                if (actual instanceof Polygon a) {
                    if (expected.getNumberOfHoles() != a.getNumberOfHoles()) {
                        return path + ": expected " + expected.getNumberOfHoles() + " holes but got " + a.getNumberOfHoles();
                    }
                    String r = compareLineCoords(path + ".shell", expected.getPolygon(), a.getPolygon());
                    if (r != null) return r;
                    for (int i = 0; i < expected.getNumberOfHoles(); i++) {
                        r = compareLineCoords(path + ".hole[" + i + "]", expected.getHole(i), a.getHole(i));
                        if (r != null) return r;
                    }
                    return null;
                }
                return typeMismatch(expected);
            }

            @Override
            public String visit(Rectangle expected) {
                // Reconstructed geometries produce Polygons, not Rectangles
                if (actual instanceof Polygon a) {
                    Polygon poly = new Polygon(
                        new LinearRing(
                            new double[] {
                                expected.getMinX(),
                                expected.getMaxX(),
                                expected.getMaxX(),
                                expected.getMinX(),
                                expected.getMinX() },
                            new double[] {
                                expected.getMinY(),
                                expected.getMinY(),
                                expected.getMaxY(),
                                expected.getMaxY(),
                                expected.getMinY() }
                        )
                    );
                    return checkEquivalent(poly, a, path);
                }
                return typeMismatch(expected);
            }

            private String typeMismatch(Geometry expectedNode) {
                return path + ": expected type " + expectedNode.type() + " but got " + actual.type();
            }

            private String sizeMismatch(int expectedSize, int actualSize) {
                return path + ": expected " + expectedSize + " elements but got " + actualSize;
            }
        }

        private String compareLineCoords(String path, Line expected, Line actual) {
            if (expected.length() != actual.length()) {
                return path + ": expected " + expected.length() + " vertices but got " + actual.length();
            }
            for (int i = 0; i < expected.length(); i++) {
                String r = compareCoord(path + "[" + i + "]", expected.getX(i), expected.getY(i), actual.getX(i), actual.getY(i));
                if (r != null) return r;
            }
            return null;
        }

        private String compareCoord(String path, double expectedX, double expectedY, double actualX, double actualY) {
            int ex = encoder.encodeX(encoder.normalizeX(expectedX));
            int ax = encoder.encodeX(encoder.normalizeX(actualX));
            if (ex != ax) {
                return path + ".x: " + expectedX + " (enc:" + ex + ") != " + actualX + " (enc:" + ax + ")";
            }
            int ey = encoder.encodeY(encoder.normalizeY(expectedY));
            int ay = encoder.encodeY(actualY);
            if (ey != ay) {
                return path + ".y: " + expectedY + " (enc:" + ey + ") != " + actualY + " (enc:" + ay + ")";
            }
            return null;
        }
    }
}
