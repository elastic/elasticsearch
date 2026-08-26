/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class FmtPercentTests extends AbstractScalarFunctionTestCase {

    public FmtPercentTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        for (var supplier : TestCaseSupplier.intCases(0, Integer.MAX_VALUE, true)) {
            suppliers.add(makeIntSupplier(supplier));
        }
        for (var supplier : TestCaseSupplier.longCases(0, Long.MAX_VALUE, true)) {
            suppliers.add(makeLongSupplier(supplier));
        }
        for (var supplier : TestCaseSupplier.doubleCases(0.0, 1.0, true)) {
            suppliers.add(makeDoubleSupplier(supplier));
        }

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new FmtPercent(source, args.get(0));
    }

    private static TestCaseSupplier makeIntSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(fieldSupplier.name(), List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            int value = ((Number) fieldTypedData.data()).intValue();
            String evaluatorToString = "FmtPercentFromIntEvaluator[value=Attribute[channel=0]]";
            double percent = value * 100.0;
            String expectedStr = formatPercent(percent);
            BytesRef expected = new BytesRef(expectedStr);
            return new TestCaseSupplier.TestCase(List.of(fieldTypedData), evaluatorToString, DataType.KEYWORD, equalTo(expected));
        });
    }

    private static TestCaseSupplier makeLongSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(fieldSupplier.name(), List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            long value = ((Number) fieldTypedData.data()).longValue();
            String evaluatorToString = "FmtPercentFromLongEvaluator[value=Attribute[channel=0]]";
            double percent = value * 100.0;
            String expectedStr = formatPercent(percent);
            BytesRef expected = new BytesRef(expectedStr);
            return new TestCaseSupplier.TestCase(List.of(fieldTypedData), evaluatorToString, DataType.KEYWORD, equalTo(expected));
        });
    }

    private static TestCaseSupplier makeDoubleSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(fieldSupplier.name(), List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            double value = ((Number) fieldTypedData.data()).doubleValue();
            String evaluatorToString = "FmtPercentFromDoubleEvaluator[value=Attribute[channel=0]]";
            double percent = value * 100.0;
            String expectedStr = formatPercent(percent);
            BytesRef expected = new BytesRef(expectedStr);
            return new TestCaseSupplier.TestCase(List.of(fieldTypedData), evaluatorToString, DataType.KEYWORD, equalTo(expected));
        });
    }

    private static String formatPercent(double percent) {
        if (percent == (long) percent) {
            return ((long) percent) + "%";
        } else {
            return String.format(java.util.Locale.ROOT, "%.1f%%", percent).replace(',', '.');
        }
    }
}
