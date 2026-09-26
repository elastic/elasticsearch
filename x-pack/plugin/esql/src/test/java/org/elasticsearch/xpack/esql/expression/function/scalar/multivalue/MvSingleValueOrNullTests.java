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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for {@link MvSingleValueOrNull}.
 * <p>
 * The function should pass through the value when the input has exactly one value,
 * and return null when the input has zero values (null) or more than one value (multi-valued).
 * </p>
 */
public class MvSingleValueOrNullTests extends AbstractMultivalueFunctionTestCase {
    public MvSingleValueOrNullTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();
        // Single-valued input passes through; multi-valued input returns null.
        booleans(
            cases,
            "single_value_or_null",
            "MvSingleValueOrNull",
            DataType.BOOLEAN,
            (size, values) -> size == 1 ? equalTo(values.findFirst().get()) : nullValue()
        );
        bytesRefs(
            cases,
            "single_value_or_null",
            "MvSingleValueOrNull",
            Function.identity(),
            (size, values) -> size == 1 ? equalTo(values.findFirst().get()) : nullValue()
        );
        doubles(
            cases,
            "single_value_or_null",
            "MvSingleValueOrNull",
            DataType.DOUBLE,
            (size, values) -> size == 1 ? equalTo(values.findFirst().getAsDouble()) : nullValue()
        );
        ints(
            cases,
            "single_value_or_null",
            "MvSingleValueOrNull",
            DataType.INTEGER,
            (size, values) -> size == 1 ? equalTo(values.findFirst().getAsInt()) : nullValue()
        );
        longs(
            cases,
            "single_value_or_null",
            "MvSingleValueOrNull",
            DataType.LONG,
            (size, values) -> size == 1 ? equalTo(values.findFirst().getAsLong()) : nullValue()
        );
        unsignedLongs(
            cases,
            "single_value_or_null",
            "MvSingleValueOrNull",
            DataType.UNSIGNED_LONG,
            (size, values) -> size == 1 ? equalTo(values.findFirst().get()) : nullValue()
        );
        dateTimes(
            cases,
            "single_value_or_null",
            "MvSingleValueOrNull",
            DataType.DATETIME,
            (size, values) -> size == 1 ? equalTo(values.findFirst().getAsLong()) : nullValue()
        );
        dateNanos(
            cases,
            "single_value_or_null",
            "MvSingleValueOrNull",
            DataType.DATE_NANOS,
            (size, values) -> size == 1 ? equalTo(values.findFirst().getAsLong()) : nullValue()
        );
        geoPoints(
            cases,
            "single_value_or_null",
            "MvSingleValueOrNull",
            DataType.GEO_POINT,
            (size, values) -> size == 1 ? equalTo(values.findFirst().get()) : nullValue()
        );
        cartesianPoints(
            cases,
            "single_value_or_null",
            "MvSingleValueOrNull",
            DataType.CARTESIAN_POINT,
            (size, values) -> size == 1 ? equalTo(values.findFirst().get()) : nullValue()
        );
        geoShape(
            cases,
            "single_value_or_null",
            "MvSingleValueOrNull",
            DataType.GEO_SHAPE,
            (size, values) -> size == 1 ? equalTo(values.findFirst().get()) : nullValue()
        );
        cartesianShape(
            cases,
            "single_value_or_null",
            "MvSingleValueOrNull",
            DataType.CARTESIAN_SHAPE,
            (size, values) -> size == 1 ? equalTo(values.findFirst().get()) : nullValue()
        );
        geohashGrid(
            cases,
            "single_value_or_null",
            "MvSingleValueOrNull",
            DataType.GEOHASH,
            (size, values) -> size == 1 ? equalTo(values.findFirst().get()) : nullValue()
        );
        geotileGrid(
            cases,
            "single_value_or_null",
            "MvSingleValueOrNull",
            DataType.GEOTILE,
            (size, values) -> size == 1 ? equalTo(values.findFirst().get()) : nullValue()
        );
        geohexGrid(
            cases,
            "single_value_or_null",
            "MvSingleValueOrNull",
            DataType.GEOHEX,
            (size, values) -> size == 1 ? equalTo(values.findFirst().get()) : nullValue()
        );
        flattened(
            cases,
            "single_value_or_null",
            "MvSingleValueOrNull",
            Function.identity(),
            (size, values) -> size == 1 ? equalTo(values.findFirst().get()) : nullValue()
        );
        // Multi-valued inputs cause MvSingleValueOrNull to emit a warning
        for (int i = 0; i < cases.size(); i++) {
            final TestCaseSupplier original = cases.get(i);
            cases.set(i, new TestCaseSupplier(original.name(), original.types(), () -> {
                TestCaseSupplier.TestCase tc = original.get();
                boolean isMultiValued = tc.getData().stream().anyMatch(td -> td.getValue() instanceof List<?> list && list.size() > 1);
                if (isMultiValued) {
                    tc = tc.withWarning(
                        "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded."
                    );
                    tc = tc.withWarning("Line 1:1: java.lang.IllegalArgumentException: single-value function encountered multi-value");
                }
                return tc;
            }));
        }
        return parameterSuppliersFromTypedDataWithDefaultChecks(false, cases);
    }

    @Override
    protected Expression build(Source source, Expression field) {
        return new MvSingleValueOrNull(source, field);
    }
}
