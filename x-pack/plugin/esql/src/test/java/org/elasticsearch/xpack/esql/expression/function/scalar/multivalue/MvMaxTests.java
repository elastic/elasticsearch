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
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class MvMaxTests extends AbstractMultivalueFunctionTestCase {
    public MvMaxTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();
        booleans(cases, "mv_max", "MvMax", (size, values) -> equalTo(values.max(Comparator.naturalOrder()).get()));
        bytesRefs(cases, "mv_max", "MvMax", (size, values) -> equalTo(values.max(Comparator.naturalOrder()).get()));
        doubles(cases, "mv_max", "MvMax", (size, values) -> equalTo(values.max().getAsDouble()));
        ints(cases, "mv_max", "MvMax", (size, values) -> equalTo(values.max().getAsInt()));
        longs(cases, "mv_max", "MvMax", (size, values) -> equalTo(values.max().getAsLong()));
        unsignedLongs(cases, "mv_max", "MvMax", (size, values) -> equalTo(values.reduce(BigInteger::max).get()));
        dateTimes(cases, "mv_max", "MvMax", (size, values) -> equalTo(values.max().getAsLong()));
        dateNanos(cases, "mv_max", "MvMax", DataType.DATE_NANOS, (size, values) -> equalTo(values.max().getAsLong()));
        return parameterSuppliersFromTypedDataWithDefaultChecks(false, cases, (v, p) -> "representableNonSpatial");
    }

    @Override
    protected Expression build(Source source, Expression field) {
        return new MvMax(source, field);
    }
}
