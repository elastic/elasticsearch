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
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class MvMinTests extends AbstractMultivalueFunctionTestCase {
    public MvMinTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();
        booleans(cases, "mv_min", "MvMin", (size, values) -> equalTo(values.min(Comparator.naturalOrder()).get()));
        bytesRefs(cases, "mv_min", "MvMin", (size, values) -> equalTo(values.min(Comparator.naturalOrder()).get()));
        doubles(cases, "mv_min", "MvMin", (size, values) -> equalTo(values.min().getAsDouble()));
        ints(cases, "mv_min", "MvMin", (size, values) -> equalTo(values.min().getAsInt()));
        longs(cases, "mv_min", "MvMin", (size, values) -> equalTo(values.min().getAsLong()));
        unsignedLongs(cases, "mv_min", "MvMin", (size, values) -> equalTo(values.reduce(BigInteger::min).get()));
        dateTimes(cases, "mv_min", "MvMin", (size, values) -> equalTo(values.min().getAsLong()));
        return parameterSuppliersFromTypedDataWithDefaultChecks(false, cases, (v, p) -> "representableNonSpatial");
    }

    @Override
    protected Expression build(Source source, Expression field) {
        return new MvMin(source, field);
    }
}
