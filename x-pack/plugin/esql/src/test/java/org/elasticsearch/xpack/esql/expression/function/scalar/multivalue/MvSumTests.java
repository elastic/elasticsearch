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
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.util.NumericUtils.asLongUnsigned;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class MvSumTests extends AbstractMultivalueFunctionTestCase {
    public MvSumTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();
        doubles(cases, "mv_sum", "MvSum", (size, values) -> equalTo(values.sum()));
        // TODO turn these on once we are summing without overflow
        // ints(cases, "mv_sum", "MvSum", (size, values) -> equalTo(values.sum()));
        // longs(cases, "mv_sum", "MvSum", (size, values) -> equalTo(values.sum()));
        // unsignedLongAsBigInteger(cases, "mv_sum", "MvSum", (size, values) -> equalTo(values.sum()));

        cases.add(arithmeticExceptionCase(DataType.INTEGER, () -> {
            List<Object> data = randomList(1, 10, () -> randomIntBetween(0, Integer.MAX_VALUE));
            data.add(Integer.MAX_VALUE);
            return data;
        }));
        cases.add(arithmeticExceptionCase(DataType.INTEGER, () -> {
            List<Object> data = randomList(1, 10, () -> randomIntBetween(Integer.MIN_VALUE, 0));
            data.add(Integer.MIN_VALUE);
            return data;
        }));
        cases.add(arithmeticExceptionCase(DataType.LONG, () -> {
            List<Object> data = randomList(1, 10, () -> randomLongBetween(0L, Long.MAX_VALUE));
            data.add(Long.MAX_VALUE);
            return data;
        }));
        cases.add(arithmeticExceptionCase(DataType.LONG, () -> {
            List<Object> data = randomList(1, 10, () -> randomLongBetween(Long.MIN_VALUE, 0L));
            data.add(Long.MIN_VALUE);
            return data;
        }));
        cases.add(arithmeticExceptionCase(DataType.UNSIGNED_LONG, () -> {
            List<Object> data = randomList(1, 10, ESTestCase::randomLong);
            data.add(asLongUnsigned(UNSIGNED_LONG_MAX));
            return data;
        }));
        return parameterSuppliersFromTypedData(cases);
    }

    private static TestCaseSupplier arithmeticExceptionCase(DataType dataType, Supplier<Object> dataSupplier) {
        String typeNameOverflow = dataType.typeName().toLowerCase(Locale.ROOT) + " overflow";
        return new TestCaseSupplier(
            "<" + typeNameOverflow + ">",
            List.of(dataType),
            () -> new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(dataSupplier.get(), dataType, "field")),
                "MvSum[field=Attribute[channel=0]]",
                dataType,
                is(nullValue())
            ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line -1:-1: java.lang.ArithmeticException: " + typeNameOverflow)
        );
    }

    @Override
    protected Expression build(Source source, Expression field) {
        return new MvSum(source, field);
    }
}
