/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geometry.Circle;
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
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.RectangleMatcher;
import org.elasticsearch.xpack.esql.expression.WellKnownBinaryBytesRefMatcher;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier.IncludingAltitude;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;

@FunctionName("st_extent")
public class SpatialStExtentTests extends AbstractAggregationTestCase {
    public SpatialStExtentTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO replace boolean with enum
        var suppliers = Stream.of(
            MultiRowTestCaseSupplier.geoPointCases(1, 1000, IncludingAltitude.NO),
            MultiRowTestCaseSupplier.cartesianPointCases(1, 1000, IncludingAltitude.NO),
            MultiRowTestCaseSupplier.geoShapeCasesWithoutCircle(1, 1000, IncludingAltitude.NO)
        ).flatMap(List::stream).map(SpatialStExtentTests::makeSupplier).toList();

        // The withNoRowsExpectingNull() cases don't work here, as this aggregator doesn't return nulls.
        // return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
        return parameterSuppliersFromTypedData(randomizeBytesRefsOffset(suppliers));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new SpatialStExtent(source, args.get(0));
    }

    private static TestCaseSupplier makeSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        if (fieldSupplier.type() != DataType.CARTESIAN_POINT && fieldSupplier.type() != DataType.GEO_POINT) {
            throw new IllegalArgumentException("Unexpected type: " + fieldSupplier.type());
        }

        return new TestCaseSupplier(List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            var values = fieldTypedData.multiRowData();

            List<Point> points = values.stream()
                .map(value -> (BytesRef) value)
                .map(value -> WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, value.bytes, value.offset, value.length))
                .flatMap(g -> g.visit(new GeometryToPointsVisitor()).stream())
                .toList();
            double minX = points.stream().mapToDouble(Point::getX).min().orElse(POSITIVE_INFINITY);
            double maxX = points.stream().mapToDouble(Point::getX).max().orElse(NEGATIVE_INFINITY);
            double maxY = points.stream().mapToDouble(Point::getY).max().orElse(NEGATIVE_INFINITY);
            double minY = points.stream().mapToDouble(Point::getY).min().orElse(NEGATIVE_INFINITY);

            return new TestCaseSupplier.TestCase(
                List.of(fieldTypedData),
                "SpatialStExtent[field=Attribute[channel=0]]",
                fieldTypedData.type(),
                new WellKnownBinaryBytesRefMatcher<Rectangle>(RectangleMatcher.closeTo(new Rectangle(minX, maxX, maxY, minY), 1e-6))
            );
        });
    }

    private static class GeometryToPointsVisitor implements GeometryVisitor<List<Point>, RuntimeException> {
        @Override
        public List<Point> visit(Circle circle) throws RuntimeException {
            throw new IllegalArgumentException("Circles are not currently supported");
        }

        @Override
        public List<Point> visit(GeometryCollection<?> collection) throws RuntimeException {
            return StreamSupport.stream(collection.spliterator(), false).flatMap(g -> g.visit(this).stream()).toList();
        }

        @Override
        public List<Point> visit(Line line) throws RuntimeException {
            return Stream.iterate(0, i -> i < line.length(), i -> i + 1)
                .map(i -> new Point(line.getX(i), line.getY(i), line.getZ(i)))
                .toList();
        }

        @Override
        public List<Point> visit(LinearRing ring) throws RuntimeException {
            return visit((Line) ring);
        }

        @Override
        public List<Point> visit(MultiLine multiLine) throws RuntimeException {
            return visit((GeometryCollection<Line>) multiLine);
        }

        @Override
        public List<Point> visit(MultiPoint multiPoint) throws RuntimeException {
            return visit((GeometryCollection<Point>) multiPoint);
        }

        @Override
        public List<Point> visit(MultiPolygon multiPolygon) throws RuntimeException {
            return visit((GeometryCollection<Polygon>) multiPolygon);
        }

        @Override
        public List<Point> visit(Point point) throws RuntimeException {
            return List.of(point);
        }

        @Override
        public List<Point> visit(Polygon polygon) throws RuntimeException {
            // We don't care about the holes as far the bounding box is concerned.
            return visit(polygon.getPolygon());
        }

        @Override
        public List<Point> visit(Rectangle rectangle) throws RuntimeException {
            return List.of(
                new Point(rectangle.getMinX(), rectangle.getMinY(), rectangle.getMinZ()),
                new Point(rectangle.getMaxX(), rectangle.getMaxY(), rectangle.getMaxZ())
            );
        }
    }
}
