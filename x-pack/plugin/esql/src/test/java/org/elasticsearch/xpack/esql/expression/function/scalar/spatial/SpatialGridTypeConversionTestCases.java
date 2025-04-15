/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.geoPointCases;

public abstract class SpatialGridTypeConversionTestCases extends AbstractScalarFunctionTestCase {
    public static <T, X> void forUnaryGeoPoint(
        DataType type,
        List<TestCaseSupplier> suppliers,
        String expectedEvaluatorToString,
        DataType expectedType,
        Function<BytesRef, T> geometryToGeotile,
        Function<Object, X> expectedValue
    ) {
        TestCaseSupplier.unary(
            suppliers,
            expectedEvaluatorToString,
            geoPointAsGeotileCases(type, geometryToGeotile),
            expectedType,
            expectedValue::apply,
            List.of()
        );
    }

    public static <T> List<TestCaseSupplier.TypedDataSupplier> geoPointAsGeotileCases(
        DataType type,
        Function<BytesRef, T> geometryToGeotile
    ) {
        return geoPointCases(ESTestCase::randomBoolean).stream()
            .map(
                t -> new TestCaseSupplier.TypedDataSupplier(
                    t.name() + " as " + type,
                    () -> geometryToGeotile.apply((BytesRef) t.supplier().get()),
                    type
                )
            )
            .toList();
    }
}
