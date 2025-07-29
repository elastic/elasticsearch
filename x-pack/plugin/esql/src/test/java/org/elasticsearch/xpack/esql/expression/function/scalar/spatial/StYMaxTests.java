/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor.WrapLongitude;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

@FunctionName("st_ymax")
public class StYMaxTests extends AbstractScalarFunctionTestCase {
    public StYMaxTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        String expectedGeo = "StYMaxFromWKBGeoEvaluator[wkb=Attribute[channel=0]]";
        String expectedCartesian = "StYMaxFromWKBEvaluator[wkb=Attribute[channel=0]]";
        final List<TestCaseSupplier> suppliers = new ArrayList<>();
        TestCaseSupplier.forUnaryGeoPoint(suppliers, expectedGeo, DOUBLE, StYMaxTests::valueOfGeo, List.of());
        TestCaseSupplier.forUnaryCartesianPoint(suppliers, expectedCartesian, DOUBLE, StYMaxTests::valueOfCartesian, List.of());
        TestCaseSupplier.forUnaryGeoShape(suppliers, expectedGeo, DOUBLE, StYMaxTests::valueOfGeo, List.of());
        TestCaseSupplier.forUnaryCartesianShape(suppliers, expectedCartesian, DOUBLE, StYMaxTests::valueOfCartesian, List.of());
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    private static double valueOfGeo(BytesRef wkb) {
        return valueOf(wkb, true);
    }

    private static double valueOfCartesian(BytesRef wkb) {
        return valueOf(wkb, false);
    }

    private static double valueOf(BytesRef wkb, boolean geo) {
        var geometry = UNSPECIFIED.wkbToGeometry(wkb);
        if (geometry instanceof Point point) {
            return point.getY();
        }
        var envelope = geo
            ? SpatialEnvelopeVisitor.visitGeo(geometry, WrapLongitude.WRAP)
            : SpatialEnvelopeVisitor.visitCartesian(geometry);
        if (envelope.isPresent()) {
            return envelope.get().getMaxY();
        }
        throw new IllegalArgumentException("Geometry is empty");
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StYMax(source, args.get(0));
    }
}
