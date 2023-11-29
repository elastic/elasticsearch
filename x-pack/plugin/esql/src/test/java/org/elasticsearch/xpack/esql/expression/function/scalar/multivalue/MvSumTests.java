/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

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
        return parameterSuppliersFromTypedData(cases);
    }

    @Override
    protected Expression build(Source source, Expression field) {
        return new MvSum(source, field);
    }

    @Override
    protected DataType[] supportedTypes() {
        return representableNumerics();
    }
}
