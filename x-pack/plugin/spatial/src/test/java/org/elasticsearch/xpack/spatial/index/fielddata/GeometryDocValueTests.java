/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.geo.Circle;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

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
        Circle tolerance = new Circle(centroidY, centroidX, 1);
        assertTrue("Expect label position to be within the geometry", shapeValue.relate(tolerance) != GeoRelation.QUERY_DISJOINT);
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
        Circle tolerance = new Circle(centroidY, centroidX, 1);
        assertTrue("Expect label position to be within the geometry", shapeValue.relate(tolerance) != GeoRelation.QUERY_DISJOINT);
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

    private static class RectangleLabelPosition extends BaseMatcher<GeoPoint> {
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
        public boolean matches(Object actual) {
            if (actual instanceof GeoPoint) {
                GeoPoint point = (GeoPoint) actual;
                int x = CoordinateEncoder.GEO.encodeX(point.lon());
                int y = CoordinateEncoder.GEO.encodeY(point.lat());
                return is(oneOf(encodedPositions)).matches(new Point(x, y));
            }
            return false;
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
}
