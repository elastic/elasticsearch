/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.Arrays;
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

        return parameterSuppliersFromTypedData(cases);
        // TODO verify why parameterSuppliersFromTypedDataWithDefaultChecks doesn't work
        // return parameterSuppliersFromTypedDataWithDefaultChecks(false, cases, (v, p) -> "double");
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvPSeriesWeightedSum(source, args.get(0), args.get(1));
    }

    private static void doubles(List<TestCaseSupplier> cases) {
        cases.add(
            new TestCaseSupplier(

                List.of(DataType.DOUBLE, DataType.DOUBLE),
                () -> new TestCaseSupplier.TestCase(
                    // TODO random inputs and outputs
                    List.of(
                        new TestCaseSupplier.TypedData(Arrays.asList(70.0, 60.0, 50.0), DataType.DOUBLE, "number"),
                        new TestCaseSupplier.TypedData(1.5, DataType.DOUBLE, "p").forceLiteral()
                    ),
                    "MvPSeriesWeightedSumDoubleEvaluator[block=Attribute[channel=0], p=1.5]",
                    DataType.DOUBLE,
                    closeTo(100.8357079220902, 0.00000001)
                )
            )
        );
    }
}
