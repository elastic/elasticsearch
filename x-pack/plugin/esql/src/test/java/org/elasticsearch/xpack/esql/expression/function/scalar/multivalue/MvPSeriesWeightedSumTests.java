/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.closeTo;

public class MvPSeriesWeightedSumTests extends AbstractScalarFunctionTestCase {
    public MvPSeriesWeightedSumTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();

        doubles(cases);

        // TODO use parameterSuppliersFromTypedDataWithDefaultChecks instead of parameterSuppliersFromTypedData and fix errors
        return parameterSuppliersFromTypedData(cases);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvPSeriesWeightedSum(source, args.get(0), args.get(1));
    }

    private static void doubles(List<TestCaseSupplier> cases) {
        cases.add(new TestCaseSupplier("most common scenario", List.of(DataType.DOUBLE, DataType.DOUBLE), () -> {
            List<Double> field = randomList(1, 10, () -> randomDoubleBetween(1, 10, false));
            double p = randomDoubleBetween(-10, 10, true);
            double expectedResult = calcPSeriesWeightedSum(field, p);

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.DOUBLE, "field"),
                    new TestCaseSupplier.TypedData(p, DataType.DOUBLE, "p").forceLiteral()
                ),
                "MvPSeriesWeightedSumDoubleEvaluator[block=Attribute[channel=0], p=" + p + "]",
                DataType.DOUBLE,
                match(expectedResult)
            );
        }));
    }

    private static Matcher<Double> match(Double value) {
        return closeTo(value, Math.abs(value * .00000001));
    }

    private static double calcPSeriesWeightedSum(List<Double> field, double p) {
        double sum = 0;
        for (int i = 0; i < field.size(); i++) {
            double current = field.get(i) / Math.pow(i + 1, p);
            sum += current;
        }
        return sum;
    }
}
