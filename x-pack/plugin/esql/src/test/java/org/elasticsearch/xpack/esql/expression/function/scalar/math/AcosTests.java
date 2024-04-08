/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.function.Supplier;

public class AcosTests extends AbstractFunctionTestCase {
    public AcosTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // values in range
        List<TestCaseSupplier> suppliers = TestCaseSupplier.forUnaryCastingToDouble("AcosEvaluator", "val", Math::acos, -1d, 1d, List.of());
        suppliers = anyNullIsNull(true, suppliers);

        // Values out of range
        suppliers.addAll(
            TestCaseSupplier.forUnaryCastingToDouble(
                "AcosEvaluator",
                "val",
                k -> null,
                Double.NEGATIVE_INFINITY,
                Math.nextDown(-1d),
                List.of(
                    "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                    "Line -1:-1: java.lang.ArithmeticException: Acos input out of range"
                )
            )
        );
        suppliers.addAll(
            TestCaseSupplier.forUnaryCastingToDouble(
                "AcosEvaluator",
                "val",
                k -> null,
                Math.nextUp(1d),
                Double.POSITIVE_INFINITY,
                List.of(
                    "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                    "Line -1:-1: java.lang.ArithmeticException: Acos input out of range"
                )
            )
        );
        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(suppliers));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Acos(source, args.get(0));
    }
}
