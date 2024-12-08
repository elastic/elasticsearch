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
import org.elasticsearch.compute.aggregation.spatial.PointType;
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
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor.WrapLongitude;
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
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@FunctionName("st_extent_agg")
public class SpatialStExtentTests extends AbstractAggregationTestCase {
    public SpatialStExtentTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = Stream.of(
            MultiRowTestCaseSupplier.geoPointCases(1, 1000, IncludingAltitude.NO),
            MultiRowTestCaseSupplier.cartesianPointCases(1, 1000, IncludingAltitude.NO),
            MultiRowTestCaseSupplier.geoShapeCasesWithoutCircle(1, 1000, IncludingAltitude.NO),
            MultiRowTestCaseSupplier.cartesianShapeCasesWithoutCircle(1, 1000, IncludingAltitude.NO)
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
        return new TestCaseSupplier(List.of(fieldSupplier.type()), () -> {
            PointType pointType = switch (fieldSupplier.type()) {
                case DataType.CARTESIAN_POINT, DataType.CARTESIAN_SHAPE -> PointType.CARTESIAN;
                case DataType.GEO_POINT, DataType.GEO_SHAPE -> PointType.GEO;
                default -> throw new IllegalArgumentException("Unsupported type: " + fieldSupplier.type());
            };
            var pointVisitor = switch (pointType) {
                case CARTESIAN -> new SpatialEnvelopeVisitor.CartesianPointVisitor();
                case GEO -> new SpatialEnvelopeVisitor.GeoPointVisitor(WrapLongitude.WRAP);
            };

            var fieldTypedData = fieldSupplier.get();
            DataType expectedType = DataType.isSpatialGeo(fieldTypedData.type()) ? DataType.GEO_SHAPE : DataType.CARTESIAN_SHAPE;
            fieldTypedData.multiRowData()
                .stream()
                .map(value -> (BytesRef) value)
                .map(value -> WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, value.bytes, value.offset, value.length))
                .forEach(g -> g.visit(new SpatialEnvelopeVisitor(pointVisitor)));
            assert pointVisitor.isValid();
            Rectangle result = pointVisitor.getResult();
            return new TestCaseSupplier.TestCase(
                List.of(fieldTypedData),
                "SpatialStExtent[field=Attribute[channel=0]]",
                expectedType,
                new WellKnownBinaryBytesRefMatcher<Rectangle>(
                    RectangleMatcher.closeTo(
                        new Rectangle(
                            // Since we use integers locally which are later decoded to doubles, all computation is effectively done using
                            // floats, not doubles.
                            (float) result.getMinX(),
                            (float) result.getMaxX(),
                            (float) result.getMaxY(),
                            (float) result.getMinY()
                        ),
                        1e-3,
                        pointType
                    )
                )
            );
        });
    }

    private static final Set<DataType> ALLOWED_SHAPES = Set.of(
        DataType.CARTESIAN_POINT,
        DataType.GEO_POINT,
        DataType.CARTESIAN_SHAPE,
        DataType.GEO_SHAPE
    );

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
