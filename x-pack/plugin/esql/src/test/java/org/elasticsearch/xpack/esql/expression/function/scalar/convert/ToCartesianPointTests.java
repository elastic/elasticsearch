/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes.CARTESIAN;

public class ToCartesianPointTests extends AbstractFunctionTestCase {
    public ToCartesianPointTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        final String attribute = "Attribute[channel=0]";
        final Function<String, String> evaluatorName = s -> "ToCartesianPoint" + s + "Evaluator[field=" + attribute + "]";
        final List<TestCaseSupplier> suppliers = new ArrayList<>();

        TestCaseSupplier.forUnaryCartesianPoint(suppliers, attribute, EsqlDataTypes.CARTESIAN_POINT, v -> v, List.of());
        TestCaseSupplier.forUnaryLong(suppliers, evaluatorName.apply("FromLong"), EsqlDataTypes.CARTESIAN_POINT, l -> {
            try {
                return CARTESIAN.longAsWKB(l);
            } catch (IllegalArgumentException e) {
                return null;
            }
        }, Long.MIN_VALUE, Long.MAX_VALUE, l -> {
            try {
                CARTESIAN.longAsWKB(l.longValue());
                return List.of();
            } catch (IllegalArgumentException exception) {
                return List.of(
                    "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                    "Line -1:-1: " + exception
                );
            }
        });
        // random strings that don't look like a cartesian point
        TestCaseSupplier.forUnaryStrings(
            suppliers,
            evaluatorName.apply("FromString"),
            EsqlDataTypes.CARTESIAN_POINT,
            bytesRef -> null,
            bytesRef -> {
                var exception = expectThrows(Exception.class, () -> CARTESIAN.stringAsWKB(bytesRef.utf8ToString()));
                return List.of(
                    "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                    "Line -1:-1: " + exception
                );
            }
        );
        // strings that are cartesian point representations
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName.apply("FromString"),
            List.of(
                new TestCaseSupplier.TypedDataSupplier(
                    "<cartesian point as string>",
                    () -> new BytesRef(CARTESIAN.pointAsString(ShapeTestUtils.randomPoint())),
                    DataTypes.KEYWORD
                )
            ),
            EsqlDataTypes.CARTESIAN_POINT,
            bytesRef -> CARTESIAN.stringAsWKB(((BytesRef) bytesRef).utf8ToString()),
            List.of()
        );

        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers)));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToCartesianPoint(source, args.get(0));
    }
}
