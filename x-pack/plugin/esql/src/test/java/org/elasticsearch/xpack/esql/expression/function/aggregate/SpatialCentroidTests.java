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
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.closeTo;

@FunctionName("st_centroid_agg")
public class SpatialCentroidTests extends AbstractAggregationTestCase {
    public SpatialCentroidTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = Stream.of(
            MultiRowTestCaseSupplier.geoPointCases(1, 1000, true),
            MultiRowTestCaseSupplier.cartesianPointCases(1, 1000, true)
        ).flatMap(List::stream).map(SpatialCentroidTests::makeSupplier).toList();

        // The withNoRowsExpectingNull() cases don't work here, as this aggregator doesn't return nulls.
        // return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
        return parameterSuppliersFromTypedData(randomizeBytesRefsOffset(suppliers));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new SpatialCentroid(source, args.get(0));
    }

    private static TestCaseSupplier makeSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        if (fieldSupplier.type() != DataType.CARTESIAN_POINT && fieldSupplier.type() != DataType.GEO_POINT) {
            throw new IllegalStateException("Unexpected type: " + fieldSupplier.type());
        }

        return new TestCaseSupplier(List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            var values = fieldTypedData.multiRowData();

            var xSum = new CompensatedSum(0, 0);
            var ySum = new CompensatedSum(0, 0);
            long count = 0;

            for (var value : values) {
                var wkb = (BytesRef) value;
                var point = (Point) WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, wkb.bytes, wkb.offset, wkb.length);
                xSum.add(point.getX());
                ySum.add(point.getY());
                count++;
            }

            var expectedX = xSum.value() / count;
            var expectedY = ySum.value() / count;

            return new TestCaseSupplier.TestCase(
                List.of(fieldTypedData),
                "SpatialCentroid[field=Attribute[channel=0]]",
                fieldTypedData.type(),
                centroidMatches(expectedX, expectedY, 1e-14)
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
