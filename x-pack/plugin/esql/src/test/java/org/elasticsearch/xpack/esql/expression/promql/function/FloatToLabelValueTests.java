/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.test.ReadableMatchers.matchesBytesRef;

/**
 * Evaluator tests for the internal {@link FloatToLabelValue} scalar behind PromQL {@code count_values}. The
 * formatting itself is checked against Go in {@link FloatToLabelValueFormatTests}.
 */
public class FloatToLabelValueTests extends AbstractScalarFunctionTestCase {

    private static final String EVALUATOR = "FloatToLabelValueEvaluator[value=Attribute[channel=0]]";

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            EVALUATOR,
            DataType.KEYWORD,
            d -> matchesBytesRef(FloatToLabelValue.format(d)),
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            List.of()
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    public FloatToLabelValueTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new FloatToLabelValue(source, args.getFirst());
    }
}
