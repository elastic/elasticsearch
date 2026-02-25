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
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.license.License;
import org.elasticsearch.lucene.spatial.CentroidCalculator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier.IncludingAltitude;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.appliesTo;
import static org.hamcrest.Matchers.closeTo;

@FunctionName("st_centroid_agg")
public class SpatialCentroidTests extends SpatialAggregationTestCase {
    public SpatialCentroidTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    public static License.OperationMode licenseRequirement(List<DataType> fieldTypes) {
        return SpatialAggregationTestCase.licenseRequirement(fieldTypes);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        // Point types (original support)
        Stream.of(
            MultiRowTestCaseSupplier.geoPointCases(1, 1000, IncludingAltitude.NO),
            MultiRowTestCaseSupplier.cartesianPointCases(1, 1000, IncludingAltitude.NO)
        ).flatMap(List::stream).map(SpatialCentroidTests::makeSupplier).forEach(suppliers::add);

        // Shape types (added in 9.4.0)
        FunctionAppliesTo shapeAppliesTo = appliesTo(FunctionAppliesToLifecycle.PREVIEW, "9.4.0", "", true);
        Stream.of(
            MultiRowTestCaseSupplier.geoShapeCasesWithoutCircle(1, 1000, IncludingAltitude.NO),
            MultiRowTestCaseSupplier.cartesianShapeCasesWithoutCircle(1, 1000, IncludingAltitude.NO)
        ).flatMap(List::stream).map(s -> s.withAppliesTo(shapeAppliesTo)).map(SpatialCentroidTests::makeSupplier).forEach(suppliers::add);

        // The withNoRowsExpectingNull() cases don't work here, as this aggregator doesn't return nulls.
        return parameterSuppliersFromTypedData(randomizeBytesRefsOffset(suppliers));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new SpatialCentroid(source, args.get(0));
    }

    private static TestCaseSupplier makeSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            var values = fieldTypedData.multiRowData();

            // Use CentroidCalculator to compute the expected centroid for all geometry types
            var calculator = new CentroidCalculator();
            for (var value : values) {
                var wkb = (BytesRef) value;
                Geometry geometry = WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, wkb.bytes, wkb.offset, wkb.length);
                calculator.add(geometry);
            }

            var expectedX = calculator.getX();
            var expectedY = calculator.getY();

            // The result type is always a point (geo_point or cartesian_point) based on the input type family
            DataType expectedType = DataType.isSpatialGeo(fieldTypedData.type()) ? DataType.GEO_POINT : DataType.CARTESIAN_POINT;

            // Use relative error for very large values (cartesian shapes can have very large coordinates)
            double absExpectedX = Math.abs(expectedX);
            double absExpectedY = Math.abs(expectedY);
            double error = Math.max(1e-10, Math.max(absExpectedX, absExpectedY) * 1e-14);

            // Both point and shape types share unified source-values aggregators
            String aggregatorName = DataType.isSpatialPoint(fieldSupplier.type()) ? "SpatialCentroidPoint" : "SpatialCentroidShape";

            return new TestCaseSupplier.TestCase(
                List.of(fieldTypedData),
                aggregatorName + "SourceValues",
                expectedType,
                centroidMatches(expectedX, expectedY, error)
            );
        });
    }

    @SuppressWarnings("SameParameterValue")
    private static Matcher<BytesRef> centroidMatches(double x, double y, double error) {
        return new TestCentroidMatcher(x, y, error);
    }

    private static class TestCentroidMatcher extends BaseMatcher<BytesRef> {
        private final double x;
        private final double y;
        private final Matcher<Double> mx;
        private final Matcher<Double> my;

        private TestCentroidMatcher(double x, double y, double error) {
            this.x = x;
            this.y = y;
            this.mx = closeTo(x, error);
            this.my = closeTo(y, error);
        }

        @Override
        public boolean matches(Object item) {
            if (item instanceof BytesRef wkb) {
                var point = (Point) WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, wkb.bytes, wkb.offset, wkb.length);
                return mx.matches(point.getX()) && my.matches(point.getY());
            }
            return false;
        }

        @Override
        public void describeMismatch(Object item, Description description) {
            if (item instanceof BytesRef wkb) {
                var point = (Point) WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, wkb.bytes, wkb.offset, wkb.length);
                description.appendText("was ").appendValue(point);
            } else {
                description.appendText("was ").appendValue(item);
            }
        }

        @Override
        public void describeTo(Description description) {
            description.appendValue("    POINT (" + x + " " + y + ")");
        }
    }
}
