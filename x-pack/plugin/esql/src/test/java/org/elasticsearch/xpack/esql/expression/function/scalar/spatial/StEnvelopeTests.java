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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

@FunctionName("st_envelope")
public class StEnvelopeTests extends AbstractScalarFunctionTestCase {
    public StEnvelopeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        String expectedGeo = "StEnvelopeFromWKBGeoEvaluator[field=Attribute[channel=0]]";
        String expectedCartesian = "StEnvelopeFromWKBEvaluator[field=Attribute[channel=0]]";
        final List<TestCaseSupplier> suppliers = new ArrayList<>();
        TestCaseSupplier.forUnaryGeoPoint(suppliers, expectedGeo, GEO_SHAPE, StEnvelopeTests::valueOfGeo, List.of());
        TestCaseSupplier.forUnaryCartesianPoint(
            suppliers,
            expectedCartesian,
            CARTESIAN_SHAPE,
            StEnvelopeTests::valueOfCartesian,
            List.of()
        );
        TestCaseSupplier.forUnaryGeoShape(suppliers, expectedGeo, GEO_SHAPE, StEnvelopeTests::valueOfGeo, List.of());
        TestCaseSupplier.forUnaryCartesianShape(
            suppliers,
            expectedCartesian,
            CARTESIAN_SHAPE,
            StEnvelopeTests::valueOfCartesian,
            List.of()
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(
            false,
            suppliers,
            (v, p) -> "geo_point, cartesian_point, geo_shape or cartesian_shape"
        );
    }

    private static BytesRef valueOfGeo(BytesRef wkb) {
        return valueOf(wkb, true);
    }

    private static BytesRef valueOfCartesian(BytesRef wkb) {
        return valueOf(wkb, false);
    }

    private static BytesRef valueOf(BytesRef wkb, boolean geo) {
        var geometry = UNSPECIFIED.wkbToGeometry(wkb);
        if (geometry instanceof Point) {
            return wkb;
        }
        var envelope = geo ? SpatialEnvelopeVisitor.visitGeo(geometry, true) : SpatialEnvelopeVisitor.visitCartesian(geometry);
        if (envelope.isPresent()) {
            return UNSPECIFIED.asWkb(envelope.get());
        }
        throw new IllegalArgumentException("Geometry is empty");
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StEnvelope(source, args.get(0));
    }
}
