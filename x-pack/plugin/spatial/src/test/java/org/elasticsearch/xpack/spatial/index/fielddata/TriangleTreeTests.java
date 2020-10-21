/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.geo.GeometryTestUtils;
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
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.geo.GeometryTestUtils.randomLine;
import static org.elasticsearch.geo.GeometryTestUtils.randomMultiLine;
import static org.elasticsearch.geo.GeometryTestUtils.randomMultiPoint;
import static org.elasticsearch.geo.GeometryTestUtils.randomMultiPolygon;
import static org.elasticsearch.geo.GeometryTestUtils.randomPoint;
import static org.elasticsearch.geo.GeometryTestUtils.randomPolygon;
import static org.hamcrest.Matchers.equalTo;

public class TriangleTreeTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testDimensionalShapeType() throws IOException {
        GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
        assertDimensionalShapeType(randomPoint(false), DimensionalShapeType.POINT);
        assertDimensionalShapeType(randomMultiPoint(false), DimensionalShapeType.POINT);
        assertDimensionalShapeType(randomLine(false), DimensionalShapeType.LINE);
        assertDimensionalShapeType(randomMultiLine(false), DimensionalShapeType.LINE);
        Geometry randoPoly = indexer.prepareForIndexing(randomValueOtherThanMany(g -> {
            try {
                Geometry newGeo = indexer.prepareForIndexing(g);
                return newGeo.type() != ShapeType.POLYGON;
            } catch (Exception e) {
                return true;
            }
        }, () -> randomPolygon(false)));
        Geometry randoMultiPoly = indexer.prepareForIndexing(randomValueOtherThanMany(g -> {
            try {
                Geometry newGeo = indexer.prepareForIndexing(g);
                return newGeo.type() != ShapeType.MULTIPOLYGON;
            } catch (Exception e) {
                return true;
            }
        }, () -> randomMultiPolygon(false)));
        assertDimensionalShapeType(randoPoly, DimensionalShapeType.POLYGON);
        assertDimensionalShapeType(randoMultiPoly, DimensionalShapeType.POLYGON);
        assertDimensionalShapeType(randomFrom(
            new GeometryCollection<>(List.of(randomPoint(false))),
            new GeometryCollection<>(List.of(randomMultiPoint(false))),
            new GeometryCollection<>(Collections.singletonList(
                new GeometryCollection<>(List.of(randomPoint(false), randomMultiPoint(false))))))
            , DimensionalShapeType.POINT);
        assertDimensionalShapeType(randomFrom(
            new GeometryCollection<>(List.of(randomPoint(false), randomLine(false))),
            new GeometryCollection<>(List.of(randomMultiPoint(false), randomMultiLine(false))),
            new GeometryCollection<>(Collections.singletonList(
                new GeometryCollection<>(List.of(randomPoint(false), randomLine(false))))))
            , DimensionalShapeType.LINE);
        assertDimensionalShapeType(randomFrom(
            new GeometryCollection<>(List.of(randomPoint(false), indexer.prepareForIndexing(randomLine(false)), randoPoly)),
            new GeometryCollection<>(List.of(randomMultiPoint(false), randoMultiPoly)),
            new GeometryCollection<>(Collections.singletonList(
                new GeometryCollection<>(List.of(indexer.prepareForIndexing(randomLine(false)),
                    indexer.prepareForIndexing(randoPoly))))))
            , DimensionalShapeType.POLYGON);
    }


    public void testRectangleShape() throws IOException {
        for (int i = 0; i < 1000; i++) {
            int minX = randomIntBetween(-40, -1);
            int maxX = randomIntBetween(1, 40);
            int minY = randomIntBetween(-40, -1);
            int maxY = randomIntBetween(1, 40);
            Geometry rectangle = new Rectangle(minX, maxX, maxY, minY);
            TriangleTreeReader reader = triangleTreeReader(rectangle, GeoShapeCoordinateEncoder.INSTANCE);

            Extent expectedExtent  = getExtentFromBox(minX, minY, maxX, maxY);
            assertThat(expectedExtent, equalTo(reader.getExtent()));
            // centroid is calculated using original double values but then loses precision as it is serialized as an integer
            int encodedCentroidX = GeoShapeCoordinateEncoder.INSTANCE.encodeX(((double) minX + maxX) / 2);
            int encodedCentroidY = GeoShapeCoordinateEncoder.INSTANCE.encodeY(((double) minY + maxY) / 2);
            assertEquals(GeoShapeCoordinateEncoder.INSTANCE.decodeX(encodedCentroidX), reader.getCentroidX(), 0.0000001);
            assertEquals(GeoShapeCoordinateEncoder.INSTANCE.decodeY(encodedCentroidY), reader.getCentroidY(), 0.0000001);

            // box-query touches bottom-left corner
            assertRelation(GeoRelation.QUERY_DISJOINT, reader, getExtentFromBox(minX - randomIntBetween(1, 180 + minX),
                minY - randomIntBetween(1, 90 + minY), minX, minY));
            // box-query touches bottom-right corner
            assertRelation(GeoRelation.QUERY_DISJOINT, reader, getExtentFromBox(maxX, minY - randomIntBetween(1, 90 + minY),
                maxX + randomIntBetween(1, 180 - maxX), minY));
            // box-query touches top-right corner
            assertRelation(GeoRelation.QUERY_DISJOINT, reader, getExtentFromBox(maxX, maxY, maxX + randomIntBetween(1, 180 - maxX),
                maxY + randomIntBetween(1, 90 - maxY)));
            // box-query touches top-left corner
            assertRelation(GeoRelation.QUERY_DISJOINT, reader, getExtentFromBox(minX - randomIntBetween(1, 180 + minX), maxY, minX,
                maxY + randomIntBetween(1, 90 - maxY)));

            // box-query fully-enclosed inside rectangle
            assertRelation(GeoRelation.QUERY_INSIDE, reader, getExtentFromBox(3 * (minX + maxX) / 4, 3 * (minY + maxY) / 4,
                3 * (maxX + minX) / 4, 3 * (maxY + minY) / 4));
            // box-query fully-contains poly
            assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(minX - randomIntBetween(1, 180 + minX),
                minY - randomIntBetween(1, 90 + minY), maxX + randomIntBetween(1, 180 - maxX),
                maxY + randomIntBetween(1, 90 - maxY)));
            // box-query half-in-half-out-right
            assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(3 * (minX + maxX) / 4, 3 * (minY + maxY) / 4,
                maxX + randomIntBetween(1, 90 - maxY), 3 * (maxY + minY) / 4));
            // box-query half-in-half-out-left
            assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(minX - randomIntBetween(1, 180 + minX),
                3 * (minY + maxY) / 4, 3 * (maxX + minX) / 4, 3 * (maxY + minY) / 4));
            // box-query half-in-half-out-top
            assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(3 * (minX + maxX) / 4, 3 * (minY + maxY) / 4,
                maxX + randomIntBetween(1, 180 - maxX), maxY + randomIntBetween(1, 90 - maxY)));
            // box-query half-in-half-out-bottom
            assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(3 * (minX + maxX) / 4,
                minY - randomIntBetween(1, 90 + minY), maxX + randomIntBetween(1, 180 - maxX),
                3 * (maxY + minY) / 4));

            // box-query outside to the right
            assertRelation(GeoRelation.QUERY_DISJOINT, reader, getExtentFromBox(maxX + randomIntBetween(1, 180 - maxX), minY,
                maxX + randomIntBetween(1, 180 - maxX), maxY));
            // box-query outside to the left
            assertRelation(GeoRelation.QUERY_DISJOINT, reader, getExtentFromBox(maxX - randomIntBetween(1, 180 - maxX), minY,
                minX - randomIntBetween(1, 180 + minX), maxY));
            // box-query outside to the top
            assertRelation(GeoRelation.QUERY_DISJOINT, reader, getExtentFromBox(minX, maxY + randomIntBetween(1, 90 - maxY), maxX,
                maxY + randomIntBetween(1, 90 - maxY)));
            // box-query outside to the bottom
            assertRelation(GeoRelation.QUERY_DISJOINT, reader, getExtentFromBox(minX, minY - randomIntBetween(1, 90 + minY), maxX,
                minY - randomIntBetween(1, 90 + minY)));
        }
    }

    public void testPacManPolygon() throws Exception {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0};
        double[] py = {0, -5, -9, -10, -9, 0, 9, 10, 9, 5, 0};

        // test cell crossing poly
        Polygon pacMan = new Polygon(new LinearRing(py, px), Collections.emptyList());
        TriangleTreeReader reader = triangleTreeReader(pacMan, TestCoordinateEncoder.INSTANCE);
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(2, -1, 11, 1));
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(-12, -12, 12, 12));
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(-2, -1, 2, 0));
        assertRelation(GeoRelation.QUERY_INSIDE, reader, getExtentFromBox(-5, -6, 2, -2));
    }

    // adapted from org.apache.lucene.geo.TestPolygon2D#testMultiPolygon
    public void testPolygonWithHole() throws Exception {
        Polygon polyWithHole = new Polygon(new LinearRing(new double[]{-50, 50, 50, -50, -50}, new double[]{-50, -50, 50, 50, -50}),
            Collections.singletonList(new LinearRing(new double[]{-10, 10, 10, -10, -10}, new double[]{-10, -10, 10, 10, -10})));

        TriangleTreeReader reader = triangleTreeReader(polyWithHole, GeoShapeCoordinateEncoder.INSTANCE);

        assertRelation(GeoRelation.QUERY_DISJOINT, reader, getExtentFromBox(6, -6, 6, -6)); // in the hole
        assertRelation(GeoRelation.QUERY_INSIDE, reader, getExtentFromBox(25, -25, 25, -25)); // on the mainland
        assertRelation(GeoRelation.QUERY_DISJOINT, reader, getExtentFromBox(51, 51, 52, 52)); // outside of mainland
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(-60, -60, 60, 60)); // enclosing us completely
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(49, 49, 51, 51)); // overlapping the mainland
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(9, 9, 11, 11)); // overlapping the hole
    }

    public void testCombPolygon() throws Exception {
        double[] px = {0, 10, 10, 20, 20, 30, 30, 40, 40, 50, 50, 0, 0};
        double[] py = {0, 0, 20, 20, 0, 0, 20, 20, 0, 0, 30, 30, 0};

        double[] hx = {21, 21, 29, 29, 21};
        double[] hy = {1, 20, 20, 1, 1};

        Polygon polyWithHole = new Polygon(new LinearRing(px, py), Collections.singletonList(new LinearRing(hx, hy)));
        TriangleTreeReader reader = triangleTreeReader(polyWithHole, GeoShapeCoordinateEncoder.INSTANCE);
        // test cell crossing poly
        assertRelation(GeoRelation.QUERY_INSIDE, reader, getExtentFromBox(5, 10, 5, 10));
        assertRelation(GeoRelation.QUERY_DISJOINT, reader, getExtentFromBox(15, 10, 15, 10));
        assertRelation(GeoRelation.QUERY_DISJOINT, reader, getExtentFromBox(25, 10, 25, 10));
    }

    public void testPacManClosedLineString() throws Exception {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0};
        double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5, 0};

        // test cell crossing poly
        TriangleTreeReader reader = triangleTreeReader(new Line(px, py), GeoShapeCoordinateEncoder.INSTANCE);
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(2, -1, 11, 1));
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(-12, -12, 12, 12));
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(-2, -1, 2, 0));
        assertRelation(GeoRelation.QUERY_DISJOINT, reader, getExtentFromBox(-5, -6, 2, -2));
    }

    public void testPacManLineString() throws Exception {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10};
        double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5};

        // test cell crossing poly
        TriangleTreeReader reader = triangleTreeReader(new Line(px, py), GeoShapeCoordinateEncoder.INSTANCE);
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(2, -1, 11, 1));
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(-12, -12, 12, 12));
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(-2, -1, 2, 0));
        assertRelation(GeoRelation.QUERY_DISJOINT, reader, getExtentFromBox(-5, -6, 2, -2));
    }

    public void testPacManPoints() throws Exception {
        // pacman
        List<Point> points = Arrays.asList(
            new Point(0, 0),
            new Point(5, 10),
            new Point(9, 10),
            new Point(10, 0),
            new Point(9, -8),
            new Point(0, -10),
            new Point(-9, -8),
            new Point(-10, 0),
            new Point(-9, 10),
            new Point(-5, 10)
        );


        // candidate intersects cell
        int xMin = 0;
        int xMax = 11;
        int yMin = -10;
        int yMax = 9;

        // test cell crossing poly
        TriangleTreeReader reader = triangleTreeReader(new MultiPoint(points), GeoShapeCoordinateEncoder.INSTANCE);
        assertRelation(GeoRelation.QUERY_CROSSES, reader, getExtentFromBox(xMin, yMin, xMax, yMax));
    }

    public void testRandomMultiLineIntersections() throws IOException {
        GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
        MultiLine geometry = randomMultiLine(false);
        geometry = (MultiLine) indexer.prepareForIndexing(geometry);
        TriangleTreeReader reader = triangleTreeReader(geometry, GeoShapeCoordinateEncoder.INSTANCE);
        Extent readerExtent = reader.getExtent();

        for (Line line : geometry) {
            Extent lineExtent = triangleTreeReader(line, GeoShapeCoordinateEncoder.INSTANCE).getExtent();
            if (lineExtent.minX() != Integer.MIN_VALUE && lineExtent.maxX() != Integer.MAX_VALUE
                && lineExtent.minY() != Integer.MIN_VALUE && lineExtent.maxY() != Integer.MAX_VALUE) {
                assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(lineExtent.minX() - 1, lineExtent.minY() - 1,
                    lineExtent.maxX() + 1, lineExtent.maxY() + 1));
            }
        }

        // extent that fully encloses the MultiLine
        assertRelation(GeoRelation.QUERY_CROSSES, reader, reader.getExtent());
        if (readerExtent.minX() != Integer.MIN_VALUE && readerExtent.maxX() != Integer.MAX_VALUE
            && readerExtent.minY() != Integer.MIN_VALUE && readerExtent.maxY() != Integer.MAX_VALUE) {
            assertRelation(GeoRelation.QUERY_CROSSES, reader, Extent.fromPoints(readerExtent.minX() - 1, readerExtent.minY() - 1,
                readerExtent.maxX() + 1, readerExtent.maxY() + 1));
        }

    }

    public void testRandomPolygonIntersection() throws IOException {
        int testPointCount = randomIntBetween(50, 100);
        Point[] testPoints = new Point[testPointCount];
        double extentSize = randomDoubleBetween(1, 10, true);
        boolean[] intersects = new boolean[testPointCount];
        for (int i = 0; i < testPoints.length; i++) {
            testPoints[i] = randomPoint(false);
        }

        Geometry geometry = randomMultiPolygon(false);
        GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
        Geometry preparedGeometry = indexer.prepareForIndexing(geometry);

        for (int i = 0; i < testPointCount; i++) {
            int cur = i;
            intersects[cur] = fold(preparedGeometry, false, (g, s) -> s || intersects(g, testPoints[cur], extentSize));
        }

        for (int i = 0; i < testPointCount; i++) {
            assertEquals(intersects[i], intersects(preparedGeometry, testPoints[i], extentSize));
        }
    }

    private Extent bufferedExtentFromGeoPoint(double x, double y, double extentSize) {
        int xMin = GeoShapeCoordinateEncoder.INSTANCE.encodeX(Math.max(x - extentSize, -180.0));
        int xMax = GeoShapeCoordinateEncoder.INSTANCE.encodeX(Math.min(x + extentSize, 180.0));
        int yMin = GeoShapeCoordinateEncoder.INSTANCE.encodeY(Math.max(y - extentSize, -90));
        int yMax = GeoShapeCoordinateEncoder.INSTANCE.encodeY(Math.min(y + extentSize, 90));
        return Extent.fromPoints(xMin, yMin, xMax, yMax);
    }

    private static Extent getExtentFromBox(double bottomLeftX, double bottomLeftY, double topRightX, double topRightY) {
        return Extent.fromPoints(GeoShapeCoordinateEncoder.INSTANCE.encodeX(bottomLeftX),
            GeoShapeCoordinateEncoder.INSTANCE.encodeY(bottomLeftY),
            GeoShapeCoordinateEncoder.INSTANCE.encodeX(topRightX),
            GeoShapeCoordinateEncoder.INSTANCE.encodeY(topRightY));

    }

    private boolean intersects(Geometry g, Point p, double extentSize) throws IOException {

        Extent bufferBounds = bufferedExtentFromGeoPoint(p.getX(), p.getY(), extentSize);
        GeoRelation relation = triangleTreeReader(g, GeoShapeCoordinateEncoder.INSTANCE)
            .relateTile(bufferBounds.minX(), bufferBounds.minY(), bufferBounds.maxX(), bufferBounds.maxY());
        return relation == GeoRelation.QUERY_CROSSES || relation == GeoRelation.QUERY_INSIDE;
    }

    private static Geometry randomGeometryTreeGeometry() {
        return randomGeometryTreeGeometry(0);
    }

    private static Geometry randomGeometryTreeGeometry(int level) {
        @SuppressWarnings("unchecked") Function<Boolean, Geometry> geometry = ESTestCase.randomFrom(
            GeometryTestUtils::randomLine,
            GeometryTestUtils::randomPoint,
            GeometryTestUtils::randomPolygon,
            GeometryTestUtils::randomMultiLine,
            GeometryTestUtils::randomMultiPoint,
            level < 3 ? (b) -> randomGeometryTreeCollection(level + 1) : GeometryTestUtils::randomPoint // don't build too deep
        );
        return geometry.apply(false);
    }

    private static Geometry randomGeometryTreeCollection(int level) {
        int size = ESTestCase.randomIntBetween(1, 10);
        List<Geometry> shapes = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            shapes.add(randomGeometryTreeGeometry(level));
        }
        return new GeometryCollection<>(shapes);
    }

    private static void assertDimensionalShapeType(Geometry geometry, DimensionalShapeType expected) throws IOException {
        TriangleTreeReader reader = triangleTreeReader(geometry, GeoShapeCoordinateEncoder.INSTANCE);
        assertThat(reader.getDimensionalShapeType(), equalTo(expected));
    }

    /**
     * Preforms left fold operation on all primitive geometries (points, lines polygons, circles and rectangles).
     * All collection geometries are iterated depth first.
     */
    public static <R, E extends Exception> R fold(Geometry geometry, R state, CheckedBiFunction<Geometry, R, R, E> operation) throws E {
        return geometry.visit(new GeometryVisitor<R, E>() {
            @Override
            public R visit(Circle circle) throws E {
                return operation.apply(geometry, state);
            }

            @Override
            public R visit(GeometryCollection<?> collection) throws E {
                R ret = state;
                for (Geometry g : collection) {
                    ret = fold(g, ret, operation);
                }
                return ret;
            }

            @Override
            public R visit(Line line) throws E {
                return operation.apply(line, state);
            }

            @Override
            public R visit(LinearRing ring) throws E {
                return operation.apply(ring, state);
            }

            @Override
            public R visit(MultiLine multiLine) throws E {
                return visit((GeometryCollection<?>) multiLine);
            }

            @Override
            public R visit(MultiPoint multiPoint) throws E {
                return visit((GeometryCollection<?>) multiPoint);            }

            @Override
            public R visit(MultiPolygon multiPolygon) throws E {
                return visit((GeometryCollection<?>) multiPolygon);
            }

            @Override
            public R visit(Point point) throws E {
                return operation.apply(point, state);
            }

            @Override
            public R visit(Polygon polygon) throws E {
                return operation.apply(polygon, state);
            }

            @Override
            public R visit(Rectangle rectangle) throws E {
                return operation.apply(rectangle, state);
            }
        });
    }

    static void assertRelation(GeoRelation expectedRelation, TriangleTreeReader reader, Extent extent) throws IOException {
        GeoRelation actualRelation = reader.relateTile(extent.minX(), extent.minY(), extent.maxX(), extent.maxY());
        assertThat(actualRelation, equalTo(expectedRelation));
    }

    static TriangleTreeReader triangleTreeReader(Geometry geometry, CoordinateEncoder encoder) throws IOException {
        ShapeField.DecodedTriangle[] triangles = GeoTestUtils.toDecodedTriangles(geometry);
        TriangleTreeWriter writer = new TriangleTreeWriter(Arrays.asList(triangles), encoder, new CentroidCalculator(geometry));
        ByteBuffersDataOutput output = new ByteBuffersDataOutput();
        writer.writeTo(output);
        TriangleTreeReader reader = new TriangleTreeReader(encoder);
        reader.reset(new BytesRef(output.toArrayCopy(), 0, Math.toIntExact(output.size())));
        return reader;
    }
}
