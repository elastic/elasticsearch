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
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor.WrapLongitude;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.license.License;
import org.elasticsearch.test.hamcrest.RectangleMatcher;
import org.elasticsearch.test.hamcrest.WellKnownBinaryBytesRefMatcher;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier.IncludingAltitude;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

@FunctionName("st_extent_agg")
public class SpatialExtentTests extends SpatialAggregationTestCase {
    public SpatialExtentTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    public static License.OperationMode licenseRequirement(List<DataType> fieldTypes) {
        return SpatialAggregationTestCase.licenseRequirement(fieldTypes);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = Stream.of(
            MultiRowTestCaseSupplier.geoPointCases(1, 1000, IncludingAltitude.NO),
            MultiRowTestCaseSupplier.cartesianPointCases(1, 1000, IncludingAltitude.NO),
            MultiRowTestCaseSupplier.geoShapeCasesWithoutCircle(1, 1000, IncludingAltitude.NO),
            MultiRowTestCaseSupplier.cartesianShapeCasesWithoutCircle(1, 1000, IncludingAltitude.NO)
        ).flatMap(List::stream).map(SpatialExtentTests::makeSupplier).toList();

        // The withNoRowsExpectingNull() cases don't work here, as this aggregator doesn't return nulls.
        return parameterSuppliersFromTypedData(randomizeBytesRefsOffset(suppliers));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new SpatialExtent(source, args.get(0));
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
                "SpatialExtent[field=Attribute[channel=0]]",
                expectedType,
                new WellKnownBinaryBytesRefMatcher<>(RectangleMatcher.closeToFloat(result, 1e-3, pointType.encoder()))
            );
        });
    }
}
