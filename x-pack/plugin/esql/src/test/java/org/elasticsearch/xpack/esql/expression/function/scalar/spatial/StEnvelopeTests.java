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
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor.WrapLongitude;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
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
        String expected = "StEnvelopeFromWKBEvaluator[wkbBlock=Attribute[channel=0]]";
        final List<TestCaseSupplier> suppliers = new ArrayList<>();
        TestCaseSupplier.forUnaryGeoPoint(suppliers, expected, GEO_SHAPE, StEnvelopeTests::valueOfGeo, List.of());
        TestCaseSupplier.forUnaryCartesianPoint(suppliers, expected, CARTESIAN_SHAPE, StEnvelopeTests::valueOfCartesian, List.of());
        TestCaseSupplier.forUnaryGeoShape(suppliers, expected, GEO_SHAPE, StEnvelopeTests::valueOfGeo, List.of());
        TestCaseSupplier.forUnaryCartesianShape(suppliers, expected, CARTESIAN_SHAPE, StEnvelopeTests::valueOfCartesian, List.of());
        return parameterSuppliersFromTypedDataWithDefaultChecks(false, suppliers);
    }

    private static BytesRef quantize(Rectangle bbox, SpatialCoordinateTypes type) {
        long encodedMin = type.pointAsLong(bbox.getMinX(), bbox.getMinY());
        long encodedMax = type.pointAsLong(bbox.getMaxX(), bbox.getMaxY());
        Rectangle quantized = new Rectangle(
            type.decodeX(encodedMin),
            type.decodeX(encodedMax),
            type.decodeY(encodedMax),
            type.decodeY(encodedMin)
        );
        return type.asWkb(quantized);
    }

    private static BytesRef valueOfGeo(BytesRef wkb) {
        return valueOf(wkb, true);
    }

    private static BytesRef valueOfCartesian(BytesRef wkb) {
        return valueOf(wkb, false);
    }

    private static BytesRef valueOf(BytesRef wkb, boolean geo) {
        var geometry = UNSPECIFIED.wkbToGeometry(wkb);
        var envelope = geo
            ? SpatialEnvelopeVisitor.visitGeo(geometry, WrapLongitude.WRAP)
            : SpatialEnvelopeVisitor.visitCartesian(geometry);
        if (envelope.isPresent()) {
            SpatialCoordinateTypes type = geo ? SpatialCoordinateTypes.GEO : SpatialCoordinateTypes.CARTESIAN;
            return quantize(envelope.get(), type);
        }
        throw new IllegalArgumentException("Geometry is empty");
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StEnvelope(source, args.getFirst());
    }
}
