/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.approximate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

public class RandomTests extends AbstractScalarFunctionTestCase {

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = List.of(new TestCaseSupplier("Basic Case", List.of(DataType.INTEGER), () -> {
            int bound = randomIntBetween(1, 100);
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(bound, DataType.INTEGER, "bound")),
                "RandomEvaluator[bound=Attribute[channel=0]]",
                DataType.INTEGER,
                allOf(instanceOf(Integer.class), greaterThanOrEqualTo(0), lessThan(bound))
            );
        }));
        return parameterSuppliersFromTypedDataWithDefaultChecks(false, suppliers);
    }

    public RandomTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Random(source, args.get(0));
    }

    @Override
    public void testFold() {
        // Random cannot be folded as it is non-deterministic.
    }
}
